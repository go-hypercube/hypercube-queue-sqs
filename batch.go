package sqsqueue

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/aws/smithy-go"
	"github.com/go-hypercube/go-hypercube/queue"
)

type itemOutcome struct {
	err        error
	definitive bool
}

type sendChunkResult struct {
	failures     map[*sendItem]itemOutcome
	operationErr error
}

func (q *Queue) sendChunk(ctx context.Context, items []*sendItem) sendChunkResult {
	result := sendChunkResult{failures: make(map[*sendItem]itemOutcome)}
	if q.client == nil {
		err := fmt.Errorf("%w: nil SQS client", queue.ErrInvalidArgument)
		for _, item := range items {
			result.failures[item] = itemOutcome{err: err, definitive: true}
		}
		return result
	}
	entries := make([]types.SendMessageBatchRequestEntry, 0, len(items))
	for index, item := range items {
		entries = append(entries, types.SendMessageBatchRequestEntry{
			Id: aws.String(batchEntryID(index)), MessageBody: aws.String(item.body),
			DelaySeconds: item.delaySeconds,
		})
	}
	output, err := q.client.SendMessageBatch(ctx, &awssqs.SendMessageBatchInput{
		QueueUrl: aws.String(items[0].queueURL), Entries: entries,
	})
	if err != nil {
		wrapped := fmt.Errorf("sqs queue: send batch: %w", err)
		result.operationErr = wrapped
		for _, item := range items {
			result.failures[item] = itemOutcome{err: wrapped}
		}
		return result
	}
	if output == nil {
		err := errors.New("sqs queue: SendMessageBatch returned a nil output")
		for _, item := range items {
			result.failures[item] = itemOutcome{err: err}
		}
		return result
	}
	parsed := batchResponseFailures(len(items), outputSuccessfulIDs(output), outputFailedEntries(output))
	for index, failure := range parsed {
		result.failures[items[index]] = failure
	}
	return result
}

func batchResponseFailures(count int, successful []string, failed []types.BatchResultErrorEntry) map[int]itemOutcome {
	result := make(map[int]itemOutcome)
	seen := make(map[int]struct{}, count)
	for _, id := range successful {
		index, ok := parseBatchEntryID(id, count)
		if !ok {
			continue
		}
		if _, duplicate := seen[index]; duplicate {
			result[index] = itemOutcome{err: fmt.Errorf("sqs queue: duplicate batch response ID %q", id)}
			continue
		}
		seen[index] = struct{}{}
	}
	for _, entry := range failed {
		id := aws.ToString(entry.Id)
		index, ok := parseBatchEntryID(id, count)
		if !ok {
			continue
		}
		if _, duplicate := seen[index]; duplicate {
			result[index] = itemOutcome{err: fmt.Errorf("sqs queue: duplicate batch response ID %q", id)}
			continue
		}
		seen[index] = struct{}{}
		result[index] = itemOutcome{err: &BatchEntryError{
			Code: aws.ToString(entry.Code), Message: aws.ToString(entry.Message), SenderFault: entry.SenderFault,
		}, definitive: true}
	}
	for index := range count {
		if _, ok := seen[index]; !ok {
			result[index] = itemOutcome{err: fmt.Errorf("sqs queue: batch response omitted entry %q", batchEntryID(index))}
		}
	}
	return result
}

func parseBatchEntryID(id string, count int) (int, bool) {
	if !strings.HasPrefix(id, "e") {
		return 0, false
	}
	index, err := strconv.Atoi(strings.TrimPrefix(id, "e"))
	return index, err == nil && index >= 0 && index < count
}

// BatchEntryError describes an item failure returned inside an otherwise
// successful SQS batch API response.
type BatchEntryError struct {
	Code        string
	Message     string
	SenderFault bool
}

func (e *BatchEntryError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("sqs queue: batch item failed (%s)", e.Code)
	}
	return fmt.Sprintf("sqs queue: batch item failed (%s): %s", e.Code, e.Message)
}

func outputSuccessfulIDs(output any) []string {
	var result []string
	switch value := output.(type) {
	case *awssqs.SendMessageBatchOutput:
		for _, entry := range value.Successful {
			result = append(result, aws.ToString(entry.Id))
		}
	case *awssqs.DeleteMessageBatchOutput:
		for _, entry := range value.Successful {
			result = append(result, aws.ToString(entry.Id))
		}
	case *awssqs.ChangeMessageVisibilityBatchOutput:
		for _, entry := range value.Successful {
			result = append(result, aws.ToString(entry.Id))
		}
	}
	return result
}

func outputFailedEntries(output any) []types.BatchResultErrorEntry {
	switch value := output.(type) {
	case *awssqs.SendMessageBatchOutput:
		return value.Failed
	case *awssqs.DeleteMessageBatchOutput:
		return value.Failed
	case *awssqs.ChangeMessageVisibilityBatchOutput:
		return value.Failed
	default:
		return nil
	}
}

func (q *Queue) releaseAfterPrepareError(
	ctx context.Context,
	queueURL string,
	messages []types.Message,
	prepareErr error,
) error {
	if err := ctx.Err(); err != nil {
		return errors.Join(prepareErr, err)
	}
	entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, 0, len(messages))
	for _, message := range messages {
		receipt := aws.ToString(message.ReceiptHandle)
		if receipt == "" {
			continue
		}
		entries = append(entries, types.ChangeMessageVisibilityBatchRequestEntry{
			Id: aws.String(batchEntryID(len(entries))), ReceiptHandle: aws.String(receipt), VisibilityTimeout: 0,
		})
	}
	if len(entries) == 0 || q.client == nil {
		return prepareErr
	}
	output, err := q.client.ChangeMessageVisibilityBatch(ctx, &awssqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(queueURL), Entries: entries,
	})
	if err != nil {
		return errors.Join(prepareErr, fmt.Errorf("sqs queue: release undelivered messages: %w", err))
	}
	if output == nil {
		return errors.Join(prepareErr, errors.New("sqs queue: release undelivered messages returned a nil output"))
	}
	failures := batchResponseFailures(len(entries), outputSuccessfulIDs(output), outputFailedEntries(output))
	if len(failures) == 0 {
		return prepareErr
	}
	joined := make([]error, 0, len(failures)+1)
	joined = append(joined, prepareErr)
	for _, failure := range failures {
		joined = append(joined, fmt.Errorf("sqs queue: release undelivered message: %w", failure.err))
	}
	return errors.Join(joined...)
}

func mapReceiptError(err error) error {
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && receiptErrorCode(apiErr.ErrorCode(), apiErr.ErrorMessage()) {
		return fmt.Errorf("%w: %w", queue.ErrNotFound, err)
	}
	return err
}

func mapBatchReceiptError(err error) error {
	var entryErr *BatchEntryError
	if errors.As(err, &entryErr) && receiptErrorCode(entryErr.Code, entryErr.Message) {
		return fmt.Errorf("%w: %w", queue.ErrNotFound, err)
	}
	return err
}

func receiptErrorCode(code, message string) bool {
	switch code {
	case "MessageNotInflight", "ReceiptHandleIsInvalid":
		return true
	case "InvalidParameterValue":
		return strings.Contains(strings.ToLower(message), "receipt")
	default:
		return false
	}
}
