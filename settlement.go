package sqsqueue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/go-hypercube/go-hypercube/queue"
)

// Ack permanently deletes current source deliveries.
func (q *Queue) Ack(ctx context.Context, msgs ...*queue.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	failed := make([]queue.ItemError, 0)
	items := make([]*sendItem, 0, len(msgs))
	for index, msg := range msgs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if msg == nil {
			failed = append(failed, itemFailure(index, nil, queue.ErrInvalidArgument))
			continue
		}
		state, err := q.currentDelivery(msg, time.Now())
		if err != nil {
			failed = append(failed, itemFailure(index, msg, err))
			continue
		}
		currentTransition, current := q.deliveryTransition(state)
		if !current {
			failed = append(failed, itemFailure(index, msg, queue.ErrNotFound))
			continue
		}
		if currentTransition != transitionNone {
			failed = append(failed, itemFailure(index, msg, ErrTransitionInProgress))
			continue
		}
		items = append(items, &sendItem{index: index, msg: msg, queueURL: state.queueURL, state: state})
	}

	for _, group := range groupSendItems(items) {
		for start := 0; start < len(group); start += MaxBatchEntries {
			end := min(start+MaxBatchEntries, len(group))
			valid := make([]*sendItem, 0, end-start)
			for _, item := range group[start:end] {
				state, err := q.currentDelivery(item.msg, time.Now())
				if err != nil || state != item.state {
					failed = append(failed, itemFailure(item.index, item.msg, queue.ErrNotFound))
					continue
				}
				currentTransition, current := q.deliveryTransition(state)
				if !current {
					failed = append(failed, itemFailure(item.index, item.msg, queue.ErrNotFound))
					continue
				}
				if currentTransition != transitionNone {
					failed = append(failed, itemFailure(item.index, item.msg, ErrTransitionInProgress))
					continue
				}
				valid = append(valid, item)
			}
			if len(valid) == 0 {
				continue
			}
			result := q.deleteChunk(ctx, valid)
			if result.operationErr != nil && isContextError(result.operationErr) {
				return contextError(ctx, result.operationErr)
			}
			for _, item := range valid {
				failure, exists := result.failures[item]
				if exists {
					err := mapBatchReceiptError(failure.err)
					if errors.Is(err, queue.ErrNotFound) {
						q.invalidateDelivery(item.state)
					}
					failed = append(failed, itemFailure(item.index, item.msg, err))
					continue
				}

				q.invalidateDelivery(item.state)
				if err := q.releaseReservation(ctx, item.state.logicalID, item.state.reservationToken); err != nil {
					failed = append(failed, itemFailure(item.index, item.msg,
						fmt.Errorf("sqs queue: release acknowledged ID reservation: %w", err)))
				}
			}
		}
	}
	return newBatchError(failed)
}

// Retry publishes each updated snapshot to its source queue and only then
// deletes the original receipt. This preserves caller changes on immutable SQS
// messages and intentionally favors duplicates over loss if deletion fails.
func (q *Queue) Retry(ctx context.Context, msgs ...*queue.Message) error {
	return q.transferSettlement(ctx, transitionRetry, msgs)
}

// DeadLetter publishes each updated snapshot to the configured explicit DLQ
// and only then deletes the original source receipt.
func (q *Queue) DeadLetter(ctx context.Context, msgs ...*queue.Message) error {
	return q.transferSettlement(ctx, transitionDeadLetter, msgs)
}

func (q *Queue) transferSettlement(ctx context.Context, operation transition, msgs []*queue.Message) error {
	if len(msgs) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	failed := make([]queue.ItemError, 0)
	failedItems := make(map[*sendItem]struct{})
	items := make([]*sendItem, 0, len(msgs))
	fresh := make([]*sendItem, 0, len(msgs))
	for index, msg := range msgs {
		if err := ctx.Err(); err != nil {
			return err
		}
		if msg == nil {
			failed = append(failed, itemFailure(index, nil, queue.ErrInvalidArgument))
			continue
		}
		state, err := q.currentDelivery(msg, time.Now())
		if err != nil {
			failed = append(failed, itemFailure(index, msg, err))
			continue
		}
		item := &sendItem{index: index, msg: msg, state: state}
		items = append(items, item)
		currentTransition, current := q.deliveryTransition(state)
		if !current {
			failed = append(failed, itemFailure(index, msg, queue.ErrNotFound))
			failedItems[item] = struct{}{}
			continue
		}
		if currentTransition != transitionNone {
			if currentTransition != operation {
				failed = append(failed, itemFailure(index, msg, ErrTransitionInProgress))
				failedItems[item] = struct{}{}
			}
			continue
		}

		if operation == transitionRetry {
			item.queueURL = state.queueURL
			item.delaySeconds, err = delaySeconds(msg.Delay)
		} else {
			item.queueURL, err = q.resolveDeadLetterURL(ctx, state.queueName)
			if err == nil && item.queueURL == state.queueURL {
				err = fmt.Errorf("%w: dead-letter queue resolves to the source queue", queue.ErrInvalidArgument)
			}
		}
		if err != nil {
			if isContextError(err) {
				return contextError(ctx, err)
			}
			failed = append(failed, itemFailure(index, msg, err))
			failedItems[item] = struct{}{}
			continue
		}
		if _, err = q.messageVisibilitySeconds(msg.VisibilityTimeout); err != nil {
			failed = append(failed, itemFailure(index, msg, err))
			failedItems[item] = struct{}{}
			continue
		}
		item.body, err = encodeMessage(msg, msg.Attempt, msg.Delay, state.reservationToken)
		if err == nil && len(item.body) > MaxMessageBytes {
			err = ErrMessageTooLarge
		}
		if err != nil {
			failed = append(failed, itemFailure(index, msg, err))
			failedItems[item] = struct{}{}
			continue
		}
		fresh = append(fresh, item)
	}

	for _, group := range groupSendItems(fresh) {
		for _, chunk := range packSendItems(group) {
			result := q.sendChunk(ctx, chunk)
			if result.operationErr != nil && isContextError(result.operationErr) {
				return contextError(ctx, result.operationErr)
			}
			for _, item := range chunk {
				if failure, exists := result.failures[item]; exists {
					failed = append(failed, itemFailure(item.index, item.msg, failure.err))
					failedItems[item] = struct{}{}
					continue
				}
				if !q.markTransition(item.state, operation) {
					failed = append(failed, itemFailure(item.index, item.msg, queue.ErrNotFound))
					failedItems[item] = struct{}{}
				}
			}
		}
	}

	toDelete := make([]*sendItem, 0, len(items))
	for _, item := range items {
		if _, failed := failedItems[item]; failed {
			continue
		}
		currentTransition, current := q.deliveryTransition(item.state)
		if !current {
			failed = append(failed, itemFailure(item.index, item.msg, queue.ErrNotFound))
			failedItems[item] = struct{}{}
			continue
		}
		if currentTransition != operation {
			failed = append(failed, itemFailure(item.index, item.msg, ErrTransitionInProgress))
			failedItems[item] = struct{}{}
			continue
		}
		item.queueURL = item.state.queueURL
		toDelete = append(toDelete, item)
	}
	for _, group := range groupSendItems(toDelete) {
		for start := 0; start < len(group); start += MaxBatchEntries {
			end := min(start+MaxBatchEntries, len(group))
			valid := make([]*sendItem, 0, end-start)
			for _, item := range group[start:end] {
				state, err := q.currentDelivery(item.msg, time.Now())
				if err != nil || state != item.state {
					failed = append(failed, itemFailure(item.index, item.msg, queue.ErrNotFound))
					continue
				}
				currentTransition, current := q.deliveryTransition(state)
				if !current {
					failed = append(failed, itemFailure(item.index, item.msg, queue.ErrNotFound))
					continue
				}
				if currentTransition != operation {
					failed = append(failed, itemFailure(item.index, item.msg, ErrTransitionInProgress))
					continue
				}
				valid = append(valid, item)
			}
			if len(valid) == 0 {
				continue
			}
			result := q.deleteChunk(ctx, valid)
			if result.operationErr != nil && isContextError(result.operationErr) {
				return contextError(ctx, result.operationErr)
			}
			for _, item := range valid {
				if failure, exists := result.failures[item]; exists {
					err := mapBatchReceiptError(failure.err)
					if errors.Is(err, queue.ErrNotFound) {
						q.invalidateDelivery(item.state)
					}
					failed = append(failed, itemFailure(item.index, item.msg, err))
					continue
				}
				q.invalidateDelivery(item.state)
			}
		}
	}
	return newBatchError(failed)
}

func (q *Queue) deliveryTransition(state *deliveryState) (transition, bool) {
	q.deliveryMu.Lock()
	defer q.deliveryMu.Unlock()
	if q.deliveries[state.tokenID] != state || q.current[state.logicalID] != state.tokenID {
		return transitionNone, false
	}
	return state.transition, true
}

func (q *Queue) markTransition(state *deliveryState, operation transition) bool {
	q.deliveryMu.Lock()
	defer q.deliveryMu.Unlock()
	if q.deliveries[state.tokenID] != state || q.current[state.logicalID] != state.tokenID {
		return false
	}
	if state.transition == transitionNone {
		state.transition = operation
	}
	return state.transition == operation
}

type deleteChunkResult struct {
	failures     map[*sendItem]itemOutcome
	operationErr error
}

func (q *Queue) deleteChunk(ctx context.Context, items []*sendItem) deleteChunkResult {
	result := deleteChunkResult{failures: make(map[*sendItem]itemOutcome)}
	if q.client == nil {
		err := fmt.Errorf("%w: nil SQS client", queue.ErrInvalidArgument)
		for _, item := range items {
			result.failures[item] = itemOutcome{err: err, definitive: true}
		}
		return result
	}
	entries := make([]types.DeleteMessageBatchRequestEntry, 0, len(items))
	for index, item := range items {
		entries = append(entries, types.DeleteMessageBatchRequestEntry{
			Id: aws.String(batchEntryID(index)), ReceiptHandle: aws.String(item.state.receiptHandle),
		})
	}
	output, err := q.client.DeleteMessageBatch(ctx, &awssqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(items[0].state.queueURL), Entries: entries,
	})
	if err != nil {
		mapped := mapReceiptError(err)
		wrapped := fmt.Errorf("sqs queue: delete batch: %w", mapped)
		result.operationErr = wrapped
		for _, item := range items {
			result.failures[item] = itemOutcome{err: wrapped}
		}
		return result
	}
	if output == nil {
		err := errors.New("sqs queue: DeleteMessageBatch returned a nil output")
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
