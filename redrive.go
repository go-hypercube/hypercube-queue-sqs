package sqsqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/go-hypercube/go-hypercube/queue"
)

// Redrive manually transfers up to count messages from queueName's explicit
// DLQ to its source queue. It publishes reset snapshots before deleting DLQ
// receipts, avoiding the loss window and immutable-body behavior of native SQS
// redrive tasks.
func (q *Queue) Redrive(ctx context.Context, queueName string, count int) (int, error) {
	if count <= 0 {
		return 0, queue.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	visibility, err := visibilitySeconds(q.defaultVisibilityTimeout)
	if err != nil {
		return 0, fmt.Errorf("sqs queue: default visibility timeout: %w", err)
	}
	sourceURL, err := q.resolveSourceURL(ctx, queueName)
	if err != nil {
		return 0, err
	}
	deadURL, err := q.resolveDeadLetterURL(ctx, queueName)
	if err != nil {
		return 0, err
	}
	if sourceURL == deadURL {
		return 0, fmt.Errorf("%w: dead-letter queue resolves to the source queue", queue.ErrInvalidArgument)
	}

	completed := 0
	for completed < count {
		output, err := q.receive(ctx, deadURL, min(count-completed, MaxBatchEntries), visibility, 0)
		if err != nil {
			return completed, fmt.Errorf("sqs queue: receive dead letters: %w", err)
		}
		if output == nil {
			return completed, errors.New("sqs queue: dead-letter ReceiveMessage returned a nil output")
		}
		if len(output.Messages) == 0 {
			return completed, nil
		}

		items, err := q.prepareRedriveItems(queueName, sourceURL, deadURL, output.Messages)
		if err != nil {
			return completed, q.releaseAfterPrepareError(ctx, deadURL, output.Messages, err)
		}
		var transferErrors []error
		for _, chunk := range packSendItems(items) {
			if err := ctx.Err(); err != nil {
				return completed, err
			}
			sendResult := q.sendChunk(ctx, chunk)
			if sendResult.operationErr != nil && isContextError(sendResult.operationErr) {
				return completed, contextError(ctx, sendResult.operationErr)
			}

			toDelete := make([]*sendItem, 0, len(chunk))
			for _, item := range chunk {
				if failure, exists := sendResult.failures[item]; exists {
					transferErrors = append(transferErrors,
						fmt.Errorf("redrive message %q: %w", item.msg.ID, failure.err))
					continue
				}
				toDelete = append(toDelete, item)
			}
			if len(toDelete) == 0 {
				continue
			}
			deleteResult := q.deleteChunk(ctx, toDelete)
			if deleteResult.operationErr != nil && isContextError(deleteResult.operationErr) {
				return completed, contextError(ctx, deleteResult.operationErr)
			}
			for _, item := range toDelete {
				if failure, exists := deleteResult.failures[item]; exists {
					transferErrors = append(transferErrors,
						fmt.Errorf("delete redriven message %q: %w", item.msg.ID, mapBatchReceiptError(failure.err)))
					continue
				}
				completed++
			}
		}
		if len(transferErrors) > 0 {
			return completed, errors.Join(transferErrors...)
		}
	}
	return completed, nil
}

func (q *Queue) prepareRedriveItems(
	queueName string,
	sourceURL string,
	deadURL string,
	messages []types.Message,
) ([]*sendItem, error) {
	items := make([]*sendItem, 0, len(messages))
	for index, raw := range messages {
		body := aws.ToString(raw.Body)
		receipt := aws.ToString(raw.ReceiptHandle)
		if body == "" || receipt == "" {
			return nil, fmt.Errorf("%w: dead letter is missing body or receipt handle", ErrMalformedMessage)
		}
		stored, err := decodeWire(body)
		if err != nil {
			return nil, err
		}
		if string(stored.QueueName) != queueName {
			return nil, fmt.Errorf("%w: dead-letter body queue %q does not match %q",
				ErrMalformedMessage, stored.QueueName, queueName)
		}
		msg := &queue.Message{
			ID: string(stored.ID), QueueName: string(stored.QueueName), Namespace: string(stored.Namespace),
			JobName: string(stored.JobName), Payload: cloneBytes(stored.Payload), Extra: cloneBytes(stored.Extra),
			VisibilityTimeout: time.Duration(stored.VisibilityNanos),
		}
		if _, err := q.messageVisibilitySeconds(msg.VisibilityTimeout); err != nil {
			return nil, fmt.Errorf("%w: stored visibility timeout: %v", ErrMalformedMessage, err)
		}
		resetBody, err := encodeMessage(msg, 0, 0, string(stored.ReservationToken))
		if err != nil {
			return nil, err
		}
		if len(resetBody) > MaxMessageBytes {
			return nil, ErrMessageTooLarge
		}
		items = append(items, &sendItem{
			index: index, msg: msg, queueURL: sourceURL, body: resetBody,
			state: &deliveryState{
				logicalID: msg.ID, queueName: queueName, queueURL: deadURL,
				receiptHandle: receipt, reservationToken: string(stored.ReservationToken),
			},
		})
	}
	return items, nil
}
