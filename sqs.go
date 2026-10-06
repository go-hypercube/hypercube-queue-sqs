// Package sqsqueue implements the Hypercube queue contract with Amazon SQS
// Standard queues. Queue resources are provisioned externally; this package
// resolves logical queue names to existing source and dead-letter queue URLs.
package sqsqueue

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/go-hypercube/go-hypercube/queue"
)

const (
	// DefaultPollingTimeout is the maximum time an empty Pop waits. SQS long
	// polling calls are split at the service's 20-second maximum.
	DefaultPollingTimeout = 20 * time.Second

	// DefaultPollingInterval prevents a tight request loop when SQS returns an
	// empty response before the requested long-poll duration.
	DefaultPollingInterval = 100 * time.Millisecond

	// DefaultVisibilityTimeout is used when Message.VisibilityTimeout is zero.
	DefaultVisibilityTimeout = 30 * time.Second

	// DefaultDeadLetterSuffix maps a logical source queue named "jobs" to an
	// explicitly managed dead-letter queue named "jobs-dlq".
	DefaultDeadLetterSuffix = "-dlq"

	MaxDelay             = 15 * time.Minute
	MaxVisibilityTimeout = 12 * time.Hour
	MaxMessageBytes      = 1 << 20
	MaxBatchEntries      = 10
	MaxLongPoll          = 20 * time.Second
)

var (
	// ErrMalformedMessage means an SQS body or required system attribute was not
	// produced by this driver or is otherwise invalid.
	ErrMalformedMessage = errors.New("sqs queue: malformed message")

	// ErrMessageTooLarge means the encoded message exceeds SQS's 1 MiB message
	// and aggregate batch payload limit.
	ErrMessageTooLarge = errors.New("sqs queue: message too large")

	// ErrFIFOQueueUnsupported is returned because FIFO queues do not support the
	// per-message delay required by the portable queue contract.
	ErrFIFOQueueUnsupported = errors.New("sqs queue: FIFO queues are not supported")

	// ErrTransitionInProgress means a publish-before-delete Retry or DeadLetter
	// already published this delivery to a different destination transition.
	ErrTransitionInProgress = errors.New("sqs queue: another settlement transition is in progress")
)

// Client is the subset of the AWS SDK v2 SQS client used by Queue.
type Client interface {
	GetQueueUrl(context.Context, *awssqs.GetQueueUrlInput, ...func(*awssqs.Options)) (*awssqs.GetQueueUrlOutput, error)
	SendMessageBatch(context.Context, *awssqs.SendMessageBatchInput, ...func(*awssqs.Options)) (*awssqs.SendMessageBatchOutput, error)
	ReceiveMessage(context.Context, *awssqs.ReceiveMessageInput, ...func(*awssqs.Options)) (*awssqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(context.Context, *awssqs.DeleteMessageBatchInput, ...func(*awssqs.Options)) (*awssqs.DeleteMessageBatchOutput, error)
	ChangeMessageVisibility(context.Context, *awssqs.ChangeMessageVisibilityInput, ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityOutput, error)
	ChangeMessageVisibilityBatch(context.Context, *awssqs.ChangeMessageVisibilityBatchInput, ...func(*awssqs.Options)) (*awssqs.ChangeMessageVisibilityBatchOutput, error)
}

var _ Client = (*awssqs.Client)(nil)

// QueueURLResolver maps a logical queue name to an existing SQS queue URL.
type QueueURLResolver func(ctx context.Context, queueName string) (string, error)

// Option configures Queue.
type Option func(*Queue)

// WithPollingTimeout sets the maximum empty Pop wait. A non-positive value
// performs one receive and returns queue.ErrEmpty if it produces no messages.
func WithPollingTimeout(timeout time.Duration) Option {
	return func(q *Queue) { q.pollingTimeout = timeout }
}

// WithPollingInterval sets the pause after an unexpectedly early empty SQS
// response. Non-positive values are ignored.
func WithPollingInterval(interval time.Duration) Option {
	return func(q *Queue) {
		if interval > 0 {
			q.pollingInterval = interval
		}
	}
}

// WithDefaultVisibilityTimeout sets the lease used for messages whose
// VisibilityTimeout is zero. Values outside (0, 12h] are retained as invalid
// configuration and reported by Pop before receiving a message.
func WithDefaultVisibilityTimeout(timeout time.Duration) Option {
	return func(q *Queue) { q.defaultVisibilityTimeout = timeout }
}

// WithIDStore replaces the process-local message-ID registry. Multiple Queue
// values can share a MemoryIDStore, while distributed deployments should use a
// strongly consistent implementation shared by every producer and consumer.
func WithIDStore(store IDStore) Option {
	return func(q *Queue) {
		if store != nil {
			q.idStore = store
		}
	}
}

// WithQueueURLResolver replaces GetQueueUrl-based source queue resolution.
// Successful resolutions are cached for the life of Queue.
func WithQueueURLResolver(resolver QueueURLResolver) Option {
	return func(q *Queue) { q.queueURLResolver = resolver }
}

// WithDeadLetterQueueURLResolver replaces the default queueName+"-dlq"
// dead-letter queue resolution. Successful resolutions are cached.
func WithDeadLetterQueueURLResolver(resolver QueueURLResolver) Option {
	return func(q *Queue) { q.deadLetterURLResolver = resolver }
}

// WithDeadLetterSuffix changes the suffix used by default dead-letter queue
// resolution. It has no effect when WithDeadLetterQueueURLResolver is set.
func WithDeadLetterSuffix(suffix string) Option {
	return func(q *Queue) { q.deadLetterSuffix = suffix }
}

// Queue is safe for concurrent use. The caller owns the AWS client. Queue does
// not provision resources or start background goroutines.
type Queue struct {
	client Client

	pollingTimeout           time.Duration
	pollingInterval          time.Duration
	defaultVisibilityTimeout time.Duration
	deadLetterSuffix         string
	idStore                  IDStore
	queueURLResolver         QueueURLResolver
	deadLetterURLResolver    QueueURLResolver
	scope                    string

	urlMu    sync.Mutex
	urlCache map[string]string

	deliveryMu sync.Mutex
	deliveries map[string]*deliveryState
	current    map[string]string
}

// SQSQueue is an alias for callers that prefer an explicit driver type name.
type SQSQueue = Queue

type deliveryToken struct {
	scope string
	value string
}

type deliveryState struct {
	tokenID          string
	logicalID        string
	queueName        string
	queueURL         string
	receiptHandle    string
	reservationToken string
	deadline         time.Time
	transition       transition
}

type transition uint8

const (
	transitionNone transition = iota
	transitionRetry
	transitionDeadLetter
)

var (
	_ queue.Queue              = (*Queue)(nil)
	_ queue.VisibilityExtender = (*Queue)(nil)
	_ queue.RedriveProvider    = (*Queue)(nil)
)

// New wraps an AWS SDK v2-compatible SQS client without contacting AWS.
func New(client Client, options ...Option) *Queue {
	q := &Queue{
		client:                   client,
		pollingTimeout:           DefaultPollingTimeout,
		pollingInterval:          DefaultPollingInterval,
		defaultVisibilityTimeout: DefaultVisibilityTimeout,
		deadLetterSuffix:         DefaultDeadLetterSuffix,
		idStore:                  NewMemoryIDStore(),
		scope:                    rand.Text(),
		urlCache:                 make(map[string]string),
		deliveries:               make(map[string]*deliveryState),
		current:                  make(map[string]string),
	}
	for _, option := range options {
		if option != nil {
			option(q)
		}
	}
	return q
}

type sendItem struct {
	index            int
	msg              *queue.Message
	queueURL         string
	body             string
	delaySeconds     int32
	reservationToken string
	state            *deliveryState
	submitted        bool
}

// Push snapshots and enqueues messages in their respective source queues.
func (q *Queue) Push(ctx context.Context, msgs ...*queue.Message) error {
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
			q.releasePrepared(context.WithoutCancel(ctx), items)
			return err
		}
		if msg == nil {
			failed = append(failed, itemFailure(index, nil, queue.ErrInvalidArgument))
			continue
		}

		reservationToken, err := q.reserveID(ctx, msg)
		if err != nil {
			if isContextError(err) {
				q.releasePrepared(context.WithoutCancel(ctx), items)
				return contextError(ctx, err)
			}
			failed = append(failed, itemFailure(index, msg, err))
			continue
		}

		delaySeconds, err := delaySeconds(msg.Delay)
		if err != nil {
			q.releaseReservation(context.WithoutCancel(ctx), msg.ID, reservationToken)
			failed = append(failed, itemFailure(index, msg, err))
			continue
		}
		if _, err := q.messageVisibilitySeconds(msg.VisibilityTimeout); err != nil {
			q.releaseReservation(context.WithoutCancel(ctx), msg.ID, reservationToken)
			failed = append(failed, itemFailure(index, msg, err))
			continue
		}
		body, err := encodeMessage(msg, 0, msg.Delay, reservationToken)
		if err != nil {
			q.releaseReservation(context.WithoutCancel(ctx), msg.ID, reservationToken)
			failed = append(failed, itemFailure(index, msg, err))
			continue
		}
		if len(body) > MaxMessageBytes {
			q.releaseReservation(context.WithoutCancel(ctx), msg.ID, reservationToken)
			failed = append(failed, itemFailure(index, msg, ErrMessageTooLarge))
			continue
		}
		queueURL, err := q.resolveSourceURL(ctx, msg.QueueName)
		if err != nil {
			q.releaseReservation(context.WithoutCancel(ctx), msg.ID, reservationToken)
			if isContextError(err) {
				q.releasePrepared(context.WithoutCancel(ctx), items)
				return contextError(ctx, err)
			}
			failed = append(failed, itemFailure(index, msg, err))
			continue
		}
		items = append(items, &sendItem{
			index: index, msg: msg, queueURL: queueURL, body: body,
			delaySeconds: delaySeconds, reservationToken: reservationToken,
		})
	}

	for _, group := range groupSendItems(items) {
		for _, chunk := range packSendItems(group) {
			if err := ctx.Err(); err != nil {
				q.releaseUnsubmitted(context.WithoutCancel(ctx), items)
				return err
			}
			for _, item := range chunk {
				item.submitted = true
			}
			result := q.sendChunk(ctx, chunk)
			if result.operationErr != nil && isContextError(result.operationErr) {
				q.releaseUnsubmitted(context.WithoutCancel(ctx), items)
				return contextError(ctx, result.operationErr)
			}
			for _, item := range chunk {
				failure, exists := result.failures[item]
				if !exists {
					continue
				}
				err := failure.err
				if failure.definitive {
					if releaseErr := q.releaseReservation(ctx, item.msg.ID, item.reservationToken); releaseErr != nil {
						err = errors.Join(err, fmt.Errorf("release ID reservation: %w", releaseErr))
					}
				}
				failed = append(failed, itemFailure(item.index, item.msg, err))
			}
		}
	}
	return newBatchError(failed)
}

// Pop long-polls queueName and returns at most ten deliveries, SQS's receive
// limit, even when maxMessages is larger.
func (q *Queue) Pop(ctx context.Context, queueName string, maxMessages int) ([]*queue.Message, error) {
	if maxMessages <= 0 {
		return nil, queue.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	defaultVisibility, err := visibilitySeconds(q.defaultVisibilityTimeout)
	if err != nil {
		return nil, fmt.Errorf("sqs queue: default visibility timeout: %w", err)
	}

	started := time.Now()
	deadline := started.Add(q.pollingTimeout)
	queueURL, err := q.resolveSourceURL(ctx, queueName)
	if err != nil {
		return nil, err
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		waitSeconds := int32(0)
		if q.pollingTimeout > 0 {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return nil, queue.ErrEmpty
			}
			waitSeconds = int32(min(remaining, MaxLongPoll) / time.Second)
		}

		requestCtx := ctx
		cancel := func() {}
		if q.pollingTimeout > 0 {
			requestCtx, cancel = context.WithDeadline(ctx, deadline)
		}
		output, receiveErr := q.receive(requestCtx, queueURL, min(maxMessages, MaxBatchEntries), defaultVisibility, waitSeconds)
		cancel()
		if receiveErr != nil {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if q.pollingTimeout > 0 && !time.Now().Before(deadline) && isContextError(receiveErr) {
				return nil, queue.ErrEmpty
			}
			return nil, receiveErr
		}
		if output == nil {
			return nil, errors.New("sqs queue: ReceiveMessage returned a nil output")
		}
		if len(output.Messages) > 0 {
			return q.prepareDeliveries(ctx, queueName, queueURL, output.Messages, defaultVisibility)
		}
		if q.pollingTimeout <= 0 || !time.Now().Before(deadline) {
			return nil, queue.ErrEmpty
		}
		if !waitUntil(ctx, min(q.pollingInterval, time.Until(deadline))) {
			return nil, ctx.Err()
		}
	}
}

func (q *Queue) receive(
	ctx context.Context,
	queueURL string,
	maxMessages int,
	defaultVisibility int32,
	waitSeconds int32,
) (*awssqs.ReceiveMessageOutput, error) {
	if q.client == nil {
		return nil, fmt.Errorf("%w: nil SQS client", queue.ErrInvalidArgument)
	}
	return q.client.ReceiveMessage(ctx, &awssqs.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: int32(maxMessages),
		VisibilityTimeout:   defaultVisibility,
		WaitTimeSeconds:     waitSeconds,
		MessageSystemAttributeNames: []types.MessageSystemAttributeName{
			types.MessageSystemAttributeNameApproximateReceiveCount,
		},
	})
}

type preparedDelivery struct {
	msg              *queue.Message
	receiptHandle    string
	reservationToken string
	visibility       int32
	deadline         time.Time
}

func (q *Queue) prepareDeliveries(
	ctx context.Context,
	queueName string,
	queueURL string,
	received []types.Message,
	defaultVisibility int32,
) ([]*queue.Message, error) {
	prepared := make([]preparedDelivery, 0, len(received))
	receivedAt := time.Now()
	for _, raw := range received {
		body := aws.ToString(raw.Body)
		receipt := aws.ToString(raw.ReceiptHandle)
		if body == "" || receipt == "" {
			return nil, q.releaseAfterPrepareError(ctx, queueURL, received,
				fmt.Errorf("%w: missing body or receipt handle", ErrMalformedMessage))
		}
		stored, err := decodeWire(body)
		if err != nil {
			return nil, q.releaseAfterPrepareError(ctx, queueURL, received, err)
		}
		if string(stored.QueueName) != queueName {
			return nil, q.releaseAfterPrepareError(ctx, queueURL, received,
				fmt.Errorf("%w: body queue %q does not match requested queue %q", ErrMalformedMessage, stored.QueueName, queueName))
		}
		countText := raw.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)]
		receiveCount, err := strconv.ParseInt(countText, 10, 64)
		if err != nil {
			return nil, q.releaseAfterPrepareError(ctx, queueURL, received,
				fmt.Errorf("%w: invalid ApproximateReceiveCount %q", ErrMalformedMessage, countText))
		}
		msg, reservationToken, err := deliveryFromWire(stored, receiveCount)
		if err != nil {
			return nil, q.releaseAfterPrepareError(ctx, queueURL, received, err)
		}
		visibility := defaultVisibility
		deadlineBase := receivedAt
		if msg.VisibilityTimeout != 0 {
			visibility, err = visibilitySeconds(msg.VisibilityTimeout)
			if err != nil {
				return nil, q.releaseAfterPrepareError(ctx, queueURL, received,
					fmt.Errorf("%w: stored visibility timeout: %v", ErrMalformedMessage, err))
			}
			deadlineBase = time.Time{}
		}
		prepared = append(prepared, preparedDelivery{
			msg: msg, receiptHandle: receipt, reservationToken: reservationToken,
			visibility: visibility, deadline: deadlineBase.Add(time.Duration(visibility) * time.Second),
		})
	}

	toAdjust := make([]preparedDelivery, 0, len(prepared))
	for _, delivery := range prepared {
		if delivery.msg.VisibilityTimeout != 0 {
			toAdjust = append(toAdjust, delivery)
		}
	}
	if len(toAdjust) > 0 {
		if err := q.adjustReceivedVisibility(ctx, queueURL, toAdjust); err != nil {
			return nil, q.releaseAfterPrepareError(ctx, queueURL, received, err)
		}
		adjustedAt := time.Now()
		for i := range prepared {
			if prepared[i].msg.VisibilityTimeout != 0 {
				prepared[i].deadline = adjustedAt.Add(time.Duration(prepared[i].visibility) * time.Second)
			}
		}
	}

	messages := make([]*queue.Message, 0, len(prepared))
	for i := range prepared {
		delivery := &prepared[i]
		state := &deliveryState{
			logicalID: delivery.msg.ID, queueName: queueName, queueURL: queueURL,
			receiptHandle: delivery.receiptHandle, reservationToken: delivery.reservationToken,
			deadline: delivery.deadline,
		}
		delivery.msg.DriverData = q.registerDelivery(state)
		messages = append(messages, delivery.msg)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}

func (q *Queue) adjustReceivedVisibility(ctx context.Context, queueURL string, deliveries []preparedDelivery) error {
	entries := make([]types.ChangeMessageVisibilityBatchRequestEntry, 0, len(deliveries))
	for index, delivery := range deliveries {
		entries = append(entries, types.ChangeMessageVisibilityBatchRequestEntry{
			Id: aws.String(batchEntryID(index)), ReceiptHandle: aws.String(delivery.receiptHandle),
			VisibilityTimeout: delivery.visibility,
		})
	}
	output, err := q.client.ChangeMessageVisibilityBatch(ctx, &awssqs.ChangeMessageVisibilityBatchInput{
		QueueUrl: aws.String(queueURL), Entries: entries,
	})
	if err != nil {
		return fmt.Errorf("sqs queue: set per-message visibility: %w", err)
	}
	if output == nil {
		return errors.New("sqs queue: ChangeMessageVisibilityBatch returned a nil output")
	}
	failures := batchResponseFailures(len(entries), outputSuccessfulIDs(output), outputFailedEntries(output))
	if len(failures) == 0 {
		return nil
	}
	errs := make([]error, 0, len(failures))
	for _, failure := range failures {
		errs = append(errs, failure.err)
	}
	return fmt.Errorf("sqs queue: set per-message visibility: %w", errors.Join(errs...))
}

// ExtendVisibility resets a current delivery's SQS visibility deadline.
func (q *Queue) ExtendVisibility(ctx context.Context, extension time.Duration, msg *queue.Message) error {
	if extension <= 0 || msg == nil {
		return queue.ErrInvalidArgument
	}
	seconds, err := visibilitySeconds(extension)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	state, err := q.currentDelivery(msg, time.Now())
	if err != nil {
		return err
	}
	currentTransition, current := q.deliveryTransition(state)
	if !current {
		return queue.ErrNotFound
	}
	if currentTransition != transitionNone {
		return ErrTransitionInProgress
	}
	if q.client == nil {
		return fmt.Errorf("%w: nil SQS client", queue.ErrInvalidArgument)
	}
	_, err = q.client.ChangeMessageVisibility(ctx, &awssqs.ChangeMessageVisibilityInput{
		QueueUrl: aws.String(state.queueURL), ReceiptHandle: aws.String(state.receiptHandle),
		VisibilityTimeout: seconds,
	})
	if err != nil {
		mapped := mapReceiptError(err)
		if errors.Is(mapped, queue.ErrNotFound) {
			q.invalidateDelivery(state)
		}
		return mapped
	}
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	q.deliveryMu.Lock()
	if current := q.deliveries[state.tokenID]; current == state {
		state.deadline = deadline
	}
	q.deliveryMu.Unlock()
	msg.DriverData = deliveryToken{scope: q.scope, value: state.tokenID}
	return nil
}

func (q *Queue) registerDelivery(state *deliveryState) deliveryToken {
	state.tokenID = rand.Text()
	q.deliveryMu.Lock()
	if previous := q.current[state.logicalID]; previous != "" {
		delete(q.deliveries, previous)
	}
	q.current[state.logicalID] = state.tokenID
	q.deliveries[state.tokenID] = state
	q.deliveryMu.Unlock()
	return deliveryToken{scope: q.scope, value: state.tokenID}
}

func (q *Queue) currentDelivery(msg *queue.Message, now time.Time) (*deliveryState, error) {
	token, ok := msg.DriverData.(deliveryToken)
	if !ok || token.scope != q.scope || token.value == "" {
		return nil, queue.ErrNotFound
	}
	q.deliveryMu.Lock()
	defer q.deliveryMu.Unlock()
	state := q.deliveries[token.value]
	if state == nil || state.logicalID != msg.ID || state.queueName != msg.QueueName || q.current[state.logicalID] != token.value {
		return nil, queue.ErrNotFound
	}
	if !now.Before(state.deadline) {
		delete(q.deliveries, token.value)
		if q.current[state.logicalID] == token.value {
			delete(q.current, state.logicalID)
		}
		return nil, queue.ErrNotFound
	}
	return state, nil
}

func (q *Queue) invalidateDelivery(state *deliveryState) {
	q.deliveryMu.Lock()
	if q.deliveries[state.tokenID] == state {
		delete(q.deliveries, state.tokenID)
	}
	if q.current[state.logicalID] == state.tokenID {
		delete(q.current, state.logicalID)
	}
	q.deliveryMu.Unlock()
}

func (q *Queue) reserveID(ctx context.Context, msg *queue.Message) (string, error) {
	if q.idStore == nil {
		return "", fmt.Errorf("%w: nil ID store", queue.ErrInvalidArgument)
	}
	if msg.ID != "" {
		token, reserved, err := q.idStore.Reserve(ctx, msg.ID)
		if err != nil {
			return "", err
		}
		if !reserved {
			return "", queue.ErrAlreadyExists
		}
		if token == "" {
			_ = q.idStore.Release(context.WithoutCancel(ctx), msg.ID, token)
			return "", fmt.Errorf("%w: ID store returned an empty reservation token", queue.ErrInvalidArgument)
		}
		return token, nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		msg.ID = rand.Text()
		token, reserved, err := q.idStore.Reserve(ctx, msg.ID)
		if err != nil {
			return "", err
		}
		if reserved {
			if token == "" {
				_ = q.idStore.Release(context.WithoutCancel(ctx), msg.ID, token)
				return "", fmt.Errorf("%w: ID store returned an empty reservation token", queue.ErrInvalidArgument)
			}
			return token, nil
		}
	}
}

func (q *Queue) releasePrepared(ctx context.Context, items []*sendItem) {
	for _, item := range items {
		if !item.submitted {
			_ = q.releaseReservation(ctx, item.msg.ID, item.reservationToken)
		}
	}
}

func (q *Queue) releaseUnsubmitted(ctx context.Context, items []*sendItem) {
	q.releasePrepared(ctx, items)
}

func (q *Queue) releaseReservation(ctx context.Context, id, token string) error {
	if q.idStore == nil {
		return fmt.Errorf("%w: nil ID store", queue.ErrInvalidArgument)
	}
	return q.idStore.Release(ctx, id, token)
}

func (q *Queue) resolveSourceURL(ctx context.Context, queueName string) (string, error) {
	return q.resolveURL(ctx, "source\x00"+queueName, queueName, q.queueURLResolver)
}

func (q *Queue) resolveDeadLetterURL(ctx context.Context, queueName string) (string, error) {
	if q.deadLetterURLResolver != nil {
		return q.resolveURL(ctx, "dead\x00"+queueName, queueName, q.deadLetterURLResolver)
	}
	deadName := queueName + q.deadLetterSuffix
	return q.resolveURL(ctx, "dead\x00"+queueName, deadName, nil)
}

func (q *Queue) resolveURL(ctx context.Context, cacheKey, name string, resolver QueueURLResolver) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	q.urlMu.Lock()
	cached := q.urlCache[cacheKey]
	q.urlMu.Unlock()
	if cached != "" {
		return cached, nil
	}

	var resolved string
	var err error
	if resolver != nil {
		resolved, err = resolver(ctx, name)
	} else {
		if q.client == nil {
			return "", fmt.Errorf("%w: nil SQS client", queue.ErrInvalidArgument)
		}
		var output *awssqs.GetQueueUrlOutput
		output, err = q.client.GetQueueUrl(ctx, &awssqs.GetQueueUrlInput{QueueName: aws.String(name)})
		if err == nil {
			if output == nil {
				err = errors.New("sqs queue: GetQueueUrl returned a nil output")
			} else {
				resolved = aws.ToString(output.QueueUrl)
			}
		}
	}
	if err != nil {
		return "", fmt.Errorf("sqs queue: resolve queue %q: %w", name, err)
	}
	if resolved == "" {
		return "", fmt.Errorf("%w: resolver returned an empty URL for queue %q", queue.ErrInvalidArgument, name)
	}
	if isFIFOQueueURL(resolved) {
		return "", fmt.Errorf("%w: %s", ErrFIFOQueueUnsupported, resolved)
	}
	q.urlMu.Lock()
	if existing := q.urlCache[cacheKey]; existing != "" {
		resolved = existing
	} else {
		q.urlCache[cacheKey] = resolved
	}
	q.urlMu.Unlock()
	return resolved, nil
}

func isFIFOQueueURL(value string) bool {
	parsed, err := url.Parse(value)
	if err == nil {
		return strings.HasSuffix(strings.TrimSuffix(parsed.Path, "/"), ".fifo")
	}
	return strings.HasSuffix(strings.TrimSuffix(value, "/"), ".fifo")
}

func delaySeconds(delay time.Duration) (int32, error) {
	if delay <= 0 {
		return 0, nil
	}
	if delay > MaxDelay {
		return 0, fmt.Errorf("%w: SQS delay %s exceeds %s", queue.ErrInvalidArgument, delay, MaxDelay)
	}
	return int32(ceilSeconds(delay)), nil
}

func visibilitySeconds(timeout time.Duration) (int32, error) {
	if timeout <= 0 {
		return 0, fmt.Errorf("%w: SQS visibility timeout must be positive", queue.ErrInvalidArgument)
	}
	if timeout > MaxVisibilityTimeout {
		return 0, fmt.Errorf("%w: SQS visibility timeout %s exceeds %s", queue.ErrInvalidArgument, timeout, MaxVisibilityTimeout)
	}
	return int32(ceilSeconds(timeout)), nil
}

func (q *Queue) messageVisibilitySeconds(timeout time.Duration) (int32, error) {
	if timeout == 0 {
		timeout = q.defaultVisibilityTimeout
	}
	return visibilitySeconds(timeout)
}

func ceilSeconds(duration time.Duration) int64 {
	seconds := int64(duration / time.Second)
	if duration%time.Second != 0 {
		seconds++
	}
	return seconds
}

func groupSendItems(items []*sendItem) [][]*sendItem {
	groups := make(map[string][]*sendItem)
	order := make([]string, 0)
	for _, item := range items {
		if _, exists := groups[item.queueURL]; !exists {
			order = append(order, item.queueURL)
		}
		groups[item.queueURL] = append(groups[item.queueURL], item)
	}
	result := make([][]*sendItem, 0, len(order))
	for _, queueURL := range order {
		result = append(result, groups[queueURL])
	}
	return result
}

func packSendItems(items []*sendItem) [][]*sendItem {
	var chunks [][]*sendItem
	for len(items) > 0 {
		bytes := 0
		end := 0
		for end < len(items) && end < MaxBatchEntries {
			next := len(items[end].body)
			if end > 0 && bytes+next > MaxMessageBytes {
				break
			}
			bytes += next
			end++
		}
		chunks = append(chunks, items[:end])
		items = items[end:]
	}
	return chunks
}

func waitUntil(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		return true
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func newBatchError(failed []queue.ItemError) error {
	if len(failed) == 0 {
		return nil
	}
	sort.SliceStable(failed, func(i, j int) bool { return failed[i].Index < failed[j].Index })
	return &queue.BatchError{Failed: failed}
}

func itemFailure(index int, msg *queue.Message, err error) queue.ItemError {
	return queue.ItemError{Index: index, Msg: msg, Err: err}
}

func batchEntryID(index int) string { return "e" + strconv.Itoa(index) }

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func contextError(ctx context.Context, fallback error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return fallback
}
