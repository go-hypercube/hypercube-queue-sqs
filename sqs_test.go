package sqsqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/go-hypercube/go-hypercube/queue"
	"github.com/stretchr/testify/require"
)

func TestQueueImplementsContracts(t *testing.T) {
	q := New(&fakeClient{})
	var _ queue.Queue = q
	var _ queue.VisibilityExtender = q
	var _ queue.RedriveProvider = q
}

func TestPushChunksSnapshotsAndDetectsDuplicates(t *testing.T) {
	client := &fakeClient{}
	q := New(client)
	ctx := context.Background()

	messages := make([]*queue.Message, 0, 13)
	for index := range 12 {
		messages = append(messages, &queue.Message{
			QueueName: "jobs", JobName: fmt.Sprintf("job-%d", index),
			Payload: []byte(fmt.Sprintf("payload-%d", index)), Attempt: 99, DriverData: "ignored",
		})
	}
	arguments := append([]*queue.Message{}, messages[:3]...)
	arguments = append(arguments, nil)
	arguments = append(arguments, messages[3:]...)

	err := q.Push(ctx, arguments...)
	batch := requireBatchError(t, err, 1)
	require.Equal(t, 3, batch.Failed[0].Index)
	require.ErrorIs(t, batch.Failed[0].Err, queue.ErrInvalidArgument)
	for _, msg := range messages {
		require.NotEmpty(t, msg.ID)
	}

	sends := client.sendCallsSnapshot()
	require.Len(t, sends, 2)
	require.Len(t, sends[0].Entries, 10)
	require.Len(t, sends[1].Entries, 2)
	require.Equal(t, "https://sqs.test/jobs", aws.ToString(sends[0].QueueUrl))

	firstBody := aws.ToString(sends[0].Entries[0].MessageBody)
	messages[0].Payload[0] = 'X'
	stored, decodeErr := decodeWire(firstBody)
	require.NoError(t, decodeErr)
	require.Equal(t, []byte("payload-0"), stored.Payload)
	require.Zero(t, stored.Attempt)
	require.NotEmpty(t, stored.ReservationToken)

	before := len(sends)
	err = q.Push(ctx, &queue.Message{ID: messages[0].ID, QueueName: "other"})
	batch = requireBatchError(t, err, 1)
	require.ErrorIs(t, batch.Failed[0].Err, queue.ErrAlreadyExists)
	require.Len(t, client.sendCallsSnapshot(), before)
}

func TestPushPacksAggregateBytesAndReleasesDefiniteFailures(t *testing.T) {
	client := &fakeClient{}
	q := New(client)
	ctx := context.Background()

	large := make([]byte, 600_000)
	require.NoError(t, q.Push(ctx,
		&queue.Message{ID: "large-one", QueueName: "jobs", Payload: large},
		&queue.Message{ID: "large-two", QueueName: "jobs", Payload: large},
	))
	require.Len(t, client.sendCallsSnapshot(), 2, "two encoded bodies cannot share SQS's 1 MiB batch budget")

	client.sendHook = func(call int, input *awssqs.SendMessageBatchInput) (*awssqs.SendMessageBatchOutput, error) {
		if call == 2 {
			return &awssqs.SendMessageBatchOutput{Failed: []types.BatchResultErrorEntry{{
				Id: input.Entries[0].Id, Code: aws.String("InvalidMessageContents"), SenderFault: true,
			}}}, nil
		}
		return successfulSend(input), nil
	}
	failed := &queue.Message{ID: "definite-failure", QueueName: "jobs"}
	batch := requireBatchError(t, q.Push(ctx, failed), 1)
	var entryErr *BatchEntryError
	require.ErrorAs(t, batch.Failed[0].Err, &entryErr)

	// An explicit failed entry was never enqueued, so its reservation was released.
	require.NoError(t, q.Push(ctx, &queue.Message{ID: failed.ID, QueueName: "jobs"}))
}

func TestPopVisibilityAckAndIDReuse(t *testing.T) {
	client := &fakeClient{}
	q := New(client, WithPollingTimeout(0))
	ctx := context.Background()
	original := &queue.Message{
		ID: "delivery", QueueName: "jobs", Namespace: "app", JobName: "work",
		Delay: 5 * time.Second, Payload: []byte("payload"), Extra: []byte("extra"),
		VisibilityTimeout: 1500 * time.Millisecond,
	}
	require.NoError(t, q.Push(ctx, original))
	body := aws.ToString(client.sendCallsSnapshot()[0].Entries[0].MessageBody)
	client.enqueueReceive(&awssqs.ReceiveMessageOutput{Messages: []types.Message{{
		Body: aws.String(body), ReceiptHandle: aws.String("receipt-1"),
		Attributes: map[string]string{string(types.MessageSystemAttributeNameApproximateReceiveCount): "2"},
	}}})

	messages, err := q.Pop(ctx, "jobs", 50)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	delivery := messages[0]
	require.Equal(t, 2, delivery.Attempt)
	require.Zero(t, delivery.Delay)
	require.Equal(t, []byte("payload"), delivery.Payload)
	require.Equal(t, []byte("extra"), delivery.Extra)
	require.NotNil(t, delivery.DriverData)

	receives := client.receiveCallsSnapshot()
	require.Equal(t, int32(MaxBatchEntries), receives[0].MaxNumberOfMessages)
	changes := client.changeBatchCallsSnapshot()
	require.Len(t, changes, 1)
	require.Equal(t, int32(2), changes[0].Entries[0].VisibilityTimeout)

	require.NoError(t, q.Ack(ctx, delivery))
	deletes := client.deleteCallsSnapshot()
	require.Len(t, deletes, 1)
	require.Equal(t, "receipt-1", aws.ToString(deletes[0].Entries[0].ReceiptHandle))

	batch := requireBatchError(t, q.Ack(ctx, delivery), 1)
	require.ErrorIs(t, batch.Failed[0].Err, queue.ErrNotFound)
	require.NoError(t, q.Push(ctx, &queue.Message{ID: original.ID, QueueName: "jobs"}))
}

func TestRetryPublishesUpdatedSnapshotBeforeDeleteAndStagesPublish(t *testing.T) {
	client := &fakeClient{}
	q := New(client, WithPollingTimeout(0))
	ctx := context.Background()
	delivery := pushAndPop(t, q, client, &queue.Message{ID: "retry", QueueName: "jobs", Payload: []byte("before")})
	delivery.Payload = []byte("after")
	delivery.Extra = []byte("metadata")
	delivery.Delay = 1500 * time.Millisecond

	client.deleteHook = func(call int, input *awssqs.DeleteMessageBatchInput) (*awssqs.DeleteMessageBatchOutput, error) {
		if call == 0 {
			return &awssqs.DeleteMessageBatchOutput{Failed: []types.BatchResultErrorEntry{{
				Id: input.Entries[0].Id, Code: aws.String("InternalError"),
			}}}, nil
		}
		return successfulDelete(input), nil
	}

	batch := requireBatchError(t, q.Retry(ctx, delivery), 1)
	require.NotErrorIs(t, batch.Failed[0].Err, queue.ErrNotFound)
	sends := client.sendCallsSnapshot()
	require.Len(t, sends, 2)
	retryEntry := sends[1].Entries[0]
	require.Equal(t, int32(2), retryEntry.DelaySeconds)
	stored, err := decodeWire(aws.ToString(retryEntry.MessageBody))
	require.NoError(t, err)
	require.Equal(t, int64(1), stored.Attempt)
	require.Equal(t, []byte("after"), stored.Payload)
	require.Equal(t, []byte("metadata"), stored.Extra)

	// The confirmed publication is staged, so retrying the failed settlement
	// only retries source deletion instead of creating another SQS copy.
	require.NoError(t, q.Retry(ctx, delivery))
	require.Len(t, client.sendCallsSnapshot(), 2)
	require.Len(t, client.deleteCallsSnapshot(), 2)
	batch = requireBatchError(t, q.Ack(ctx, delivery), 1)
	require.ErrorIs(t, batch.Failed[0].Err, queue.ErrNotFound)
}

func TestDeadLetterPublishesUpdatedStateToExplicitDLQ(t *testing.T) {
	client := &fakeClient{}
	q := New(client, WithPollingTimeout(0))
	ctx := context.Background()
	delivery := pushAndPop(t, q, client, &queue.Message{ID: "dead", QueueName: "jobs", Payload: []byte("before")})
	delivery.Payload = []byte("after")
	delivery.Extra = []byte("failed")

	require.NoError(t, q.DeadLetter(ctx, delivery))
	sends := client.sendCallsSnapshot()
	require.Len(t, sends, 2)
	require.Equal(t, "https://sqs.test/jobs-dlq", aws.ToString(sends[1].QueueUrl))
	stored, err := decodeWire(aws.ToString(sends[1].Entries[0].MessageBody))
	require.NoError(t, err)
	require.Equal(t, int64(1), stored.Attempt)
	require.Equal(t, []byte("after"), stored.Payload)
	require.Equal(t, []byte("failed"), stored.Extra)
	require.Len(t, client.deleteCallsSnapshot(), 1)
}

func TestExtendVisibilityAndStaleSettlement(t *testing.T) {
	client := &fakeClient{}
	q := New(client, WithPollingTimeout(0))
	ctx := context.Background()
	delivery := pushAndPop(t, q, client, &queue.Message{ID: "visibility", QueueName: "jobs"})

	require.ErrorIs(t, q.ExtendVisibility(ctx, 0, delivery), queue.ErrInvalidArgument)
	require.NoError(t, q.ExtendVisibility(ctx, 1200*time.Millisecond, delivery))
	single := client.changeCallsSnapshot()
	require.Len(t, single, 1)
	require.Equal(t, int32(2), single[0].VisibilityTimeout)

	token := delivery.DriverData.(deliveryToken)
	q.deliveryMu.Lock()
	q.deliveries[token.value].deadline = time.Now().Add(-time.Second)
	q.deliveryMu.Unlock()
	batch := requireBatchError(t, q.Ack(ctx, delivery), 1)
	require.ErrorIs(t, batch.Failed[0].Err, queue.ErrNotFound)
	require.Empty(t, client.deleteCallsSnapshot())
}

func TestReceiptFailureMapsToNotFound(t *testing.T) {
	client := &fakeClient{}
	q := New(client, WithPollingTimeout(0))
	ctx := context.Background()
	delivery := pushAndPop(t, q, client, &queue.Message{ID: "stale", QueueName: "jobs"})
	client.deleteHook = func(_ int, input *awssqs.DeleteMessageBatchInput) (*awssqs.DeleteMessageBatchOutput, error) {
		return &awssqs.DeleteMessageBatchOutput{Failed: []types.BatchResultErrorEntry{{
			Id: input.Entries[0].Id, Code: aws.String("ReceiptHandleIsInvalid"), SenderFault: true,
		}}}, nil
	}
	batch := requireBatchError(t, q.Ack(ctx, delivery), 1)
	require.ErrorIs(t, batch.Failed[0].Err, queue.ErrNotFound)
}

func TestPopWaitsForConfiguredTimeoutAndObservesCancellation(t *testing.T) {
	client := &fakeClient{}
	q := New(client, WithPollingTimeout(35*time.Millisecond), WithPollingInterval(5*time.Millisecond))
	started := time.Now()
	messages, err := q.Pop(context.Background(), "empty", 1)
	require.Nil(t, messages)
	require.ErrorIs(t, err, queue.ErrEmpty)
	require.GreaterOrEqual(t, time.Since(started), 25*time.Millisecond)
	require.Greater(t, len(client.receiveCallsSnapshot()), 1)

	blocking := &fakeClient{}
	blocking.receiveHook = func(_ int, ctx context.Context, _ *awssqs.ReceiveMessageInput) (*awssqs.ReceiveMessageOutput, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	q = New(blocking)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(10*time.Millisecond, cancel)
	messages, err = q.Pop(ctx, "jobs", 1)
	require.Nil(t, messages)
	require.ErrorIs(t, err, context.Canceled)
}

func TestRedrivePublishesResetSnapshotBeforeDeletingDLQ(t *testing.T) {
	client := &fakeClient{}
	q := New(client, WithPollingTimeout(0))
	ctx := context.Background()
	body, err := encodeMessage(&queue.Message{
		ID: "redrive", QueueName: "jobs", Namespace: "app", JobName: "work",
		Payload: []byte("payload"), Extra: []byte("failure"), Delay: time.Minute,
		VisibilityTimeout: time.Minute,
	}, 7, time.Minute, "reservation")
	require.NoError(t, err)
	client.enqueueReceive(&awssqs.ReceiveMessageOutput{Messages: []types.Message{{
		Body: aws.String(body), ReceiptHandle: aws.String("dead-receipt"),
		Attributes: map[string]string{string(types.MessageSystemAttributeNameApproximateReceiveCount): "1"},
	}}})

	n, err := q.Redrive(ctx, "jobs", 1)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	sends := client.sendCallsSnapshot()
	require.Len(t, sends, 1)
	require.Equal(t, "https://sqs.test/jobs", aws.ToString(sends[0].QueueUrl))
	reset, err := decodeWire(aws.ToString(sends[0].Entries[0].MessageBody))
	require.NoError(t, err)
	require.Zero(t, reset.Attempt)
	require.Zero(t, reset.DelayNanos)
	require.Equal(t, []byte("failure"), reset.Extra)
	deletes := client.deleteCallsSnapshot()
	require.Len(t, deletes, 1)
	require.Equal(t, "https://sqs.test/jobs-dlq", aws.ToString(deletes[0].QueueUrl))
	require.Equal(t, "dead-receipt", aws.ToString(deletes[0].Entries[0].ReceiptHandle))
}

func TestEmptyBatchesIgnoreCanceledContextAndConcurrentDuplicateID(t *testing.T) {
	client := &fakeClient{}
	q := New(client)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.NoError(t, q.Push(ctx))
	require.NoError(t, q.Ack(ctx))
	require.NoError(t, q.Retry(ctx))
	require.NoError(t, q.DeadLetter(ctx))

	const workers = 16
	start := make(chan struct{})
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- q.Push(context.Background(), &queue.Message{ID: "shared", QueueName: "jobs"})
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	successes := 0
	duplicates := 0
	for err := range results {
		if err == nil {
			successes++
			continue
		}
		batch := requireBatchError(t, err, 1)
		if errors.Is(batch.Failed[0].Err, queue.ErrAlreadyExists) {
			duplicates++
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, workers-1, duplicates)
}

func pushAndPop(t *testing.T, q *Queue, client *fakeClient, msg *queue.Message) *queue.Message {
	t.Helper()
	require.NoError(t, q.Push(context.Background(), msg))
	sends := client.sendCallsSnapshot()
	body := aws.ToString(sends[len(sends)-1].Entries[0].MessageBody)
	client.enqueueReceive(&awssqs.ReceiveMessageOutput{Messages: []types.Message{{
		Body: aws.String(body), ReceiptHandle: aws.String("receipt-" + msg.ID),
		Attributes: map[string]string{string(types.MessageSystemAttributeNameApproximateReceiveCount): "1"},
	}}})
	messages, err := q.Pop(context.Background(), msg.QueueName, 1)
	require.NoError(t, err)
	require.Len(t, messages, 1)
	return messages[0]
}

func requireBatchError(t *testing.T, err error, count int) *queue.BatchError {
	t.Helper()
	require.Error(t, err)
	var batch *queue.BatchError
	require.ErrorAs(t, err, &batch)
	require.Len(t, batch.Failed, count)
	return batch
}

type fakeClient struct {
	mu sync.Mutex

	sendCalls        []*awssqs.SendMessageBatchInput
	receiveCalls     []*awssqs.ReceiveMessageInput
	deleteCalls      []*awssqs.DeleteMessageBatchInput
	changeCalls      []*awssqs.ChangeMessageVisibilityInput
	changeBatchCalls []*awssqs.ChangeMessageVisibilityBatchInput
	receiveOutputs   []*awssqs.ReceiveMessageOutput

	sendHook        func(call int, input *awssqs.SendMessageBatchInput) (*awssqs.SendMessageBatchOutput, error)
	receiveHook     func(call int, ctx context.Context, input *awssqs.ReceiveMessageInput) (*awssqs.ReceiveMessageOutput, error)
	deleteHook      func(call int, input *awssqs.DeleteMessageBatchInput) (*awssqs.DeleteMessageBatchOutput, error)
	changeHook      func(call int, input *awssqs.ChangeMessageVisibilityInput) (*awssqs.ChangeMessageVisibilityOutput, error)
	changeBatchHook func(call int, input *awssqs.ChangeMessageVisibilityBatchInput) (*awssqs.ChangeMessageVisibilityBatchOutput, error)
}

var _ Client = (*fakeClient)(nil)

func (f *fakeClient) GetQueueUrl(_ context.Context, input *awssqs.GetQueueUrlInput, _ ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error) {
	return &awssqs.GetQueueUrlOutput{QueueUrl: aws.String("https://sqs.test/" + aws.ToString(input.QueueName))}, nil
}

func (f *fakeClient) SendMessageBatch(_ context.Context, input *awssqs.SendMessageBatchInput, _ ...func(*awssqs.Options)) (*awssqs.SendMessageBatchOutput, error) {
	f.mu.Lock()
	call := len(f.sendCalls)
	f.sendCalls = append(f.sendCalls, input)
	hook := f.sendHook
	f.mu.Unlock()
	if hook != nil {
		return hook(call, input)
	}
	return successfulSend(input), nil
}

func (f *fakeClient) ReceiveMessage(ctx context.Context, input *awssqs.ReceiveMessageInput, _ ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	call := len(f.receiveCalls)
	f.receiveCalls = append(f.receiveCalls, input)
	hook := f.receiveHook
	var output *awssqs.ReceiveMessageOutput
	if len(f.receiveOutputs) > 0 {
		output = f.receiveOutputs[0]
		f.receiveOutputs = f.receiveOutputs[1:]
	}
	f.mu.Unlock()
	if hook != nil {
		return hook(call, ctx, input)
	}
	if output == nil {
		output = &awssqs.ReceiveMessageOutput{}
	}
	return output, nil
}

func (f *fakeClient) DeleteMessageBatch(_ context.Context, input *awssqs.DeleteMessageBatchInput, _ ...func(*awssqs.Options)) (*awssqs.DeleteMessageBatchOutput, error) {
	f.mu.Lock()
	call := len(f.deleteCalls)
	f.deleteCalls = append(f.deleteCalls, input)
	hook := f.deleteHook
	f.mu.Unlock()
	if hook != nil {
		return hook(call, input)
	}
	return successfulDelete(input), nil
}

func (f *fakeClient) ChangeMessageVisibility(_ context.Context, input *awssqs.ChangeMessageVisibilityInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error) {
	f.mu.Lock()
	call := len(f.changeCalls)
	f.changeCalls = append(f.changeCalls, input)
	hook := f.changeHook
	f.mu.Unlock()
	if hook != nil {
		return hook(call, input)
	}
	return &awssqs.ChangeMessageVisibilityOutput{}, nil
}

func (f *fakeClient) ChangeMessageVisibilityBatch(_ context.Context, input *awssqs.ChangeMessageVisibilityBatchInput, _ ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityBatchOutput, error) {
	f.mu.Lock()
	call := len(f.changeBatchCalls)
	f.changeBatchCalls = append(f.changeBatchCalls, input)
	hook := f.changeBatchHook
	f.mu.Unlock()
	if hook != nil {
		return hook(call, input)
	}
	output := &awssqs.ChangeMessageVisibilityBatchOutput{}
	for _, entry := range input.Entries {
		output.Successful = append(output.Successful, types.ChangeMessageVisibilityBatchResultEntry{Id: entry.Id})
	}
	return output, nil
}

func (f *fakeClient) enqueueReceive(output *awssqs.ReceiveMessageOutput) {
	f.mu.Lock()
	f.receiveOutputs = append(f.receiveOutputs, output)
	f.mu.Unlock()
}

func (f *fakeClient) sendCallsSnapshot() []*awssqs.SendMessageBatchInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*awssqs.SendMessageBatchInput(nil), f.sendCalls...)
}

func (f *fakeClient) receiveCallsSnapshot() []*awssqs.ReceiveMessageInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*awssqs.ReceiveMessageInput(nil), f.receiveCalls...)
}

func (f *fakeClient) deleteCallsSnapshot() []*awssqs.DeleteMessageBatchInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*awssqs.DeleteMessageBatchInput(nil), f.deleteCalls...)
}

func (f *fakeClient) changeCallsSnapshot() []*awssqs.ChangeMessageVisibilityInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*awssqs.ChangeMessageVisibilityInput(nil), f.changeCalls...)
}

func (f *fakeClient) changeBatchCallsSnapshot() []*awssqs.ChangeMessageVisibilityBatchInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*awssqs.ChangeMessageVisibilityBatchInput(nil), f.changeBatchCalls...)
}

func successfulSend(input *awssqs.SendMessageBatchInput) *awssqs.SendMessageBatchOutput {
	output := &awssqs.SendMessageBatchOutput{}
	for _, entry := range input.Entries {
		output.Successful = append(output.Successful, types.SendMessageBatchResultEntry{Id: entry.Id})
	}
	return output
}

func successfulDelete(input *awssqs.DeleteMessageBatchInput) *awssqs.DeleteMessageBatchOutput {
	output := &awssqs.DeleteMessageBatchOutput{}
	for _, entry := range input.Entries {
		output.Successful = append(output.Successful, types.DeleteMessageBatchResultEntry{Id: entry.Id})
	}
	return output
}
