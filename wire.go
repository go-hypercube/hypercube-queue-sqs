package sqsqueue

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/go-hypercube/go-hypercube/queue"
)

const wireVersion = 1

// String fields are encoded as byte slices so arbitrary Go strings survive
// JSON's UTF-8 requirements. Payload and Extra are base64-encoded by json too.
type wireMessage struct {
	Version          int    `json:"v"`
	ID               []byte `json:"id"`
	QueueName        []byte `json:"queue"`
	Namespace        []byte `json:"namespace,omitempty"`
	JobName          []byte `json:"job,omitempty"`
	Payload          []byte `json:"payload"`
	Extra            []byte `json:"extra"`
	Attempt          int64  `json:"attempt"`
	DelayNanos       int64  `json:"delay_nanos"`
	VisibilityNanos  int64  `json:"visibility_nanos"`
	ReservationToken []byte `json:"reservation_token"`
}

func encodeMessage(msg *queue.Message, attempt int, delay time.Duration, reservationToken string) (string, error) {
	if attempt < 0 {
		return "", fmt.Errorf("%w: negative attempt", queue.ErrInvalidArgument)
	}
	encoded, err := json.Marshal(wireMessage{
		Version:          wireVersion,
		ID:               []byte(msg.ID),
		QueueName:        []byte(msg.QueueName),
		Namespace:        []byte(msg.Namespace),
		JobName:          []byte(msg.JobName),
		Payload:          msg.Payload,
		Extra:            msg.Extra,
		Attempt:          int64(attempt),
		DelayNanos:       int64(delay),
		VisibilityNanos:  int64(msg.VisibilityTimeout),
		ReservationToken: []byte(reservationToken),
	})
	if err != nil {
		return "", fmt.Errorf("sqs queue: encode message %q: %w", msg.ID, err)
	}
	return string(encoded), nil
}

func decodeWire(body string) (wireMessage, error) {
	var stored wireMessage
	if err := json.Unmarshal([]byte(body), &stored); err != nil {
		return wireMessage{}, fmt.Errorf("%w: decode body: %v", ErrMalformedMessage, err)
	}
	if stored.Version != wireVersion {
		return wireMessage{}, fmt.Errorf("%w: unsupported wire version %d", ErrMalformedMessage, stored.Version)
	}
	if len(stored.ID) == 0 {
		return wireMessage{}, fmt.Errorf("%w: empty logical message ID", ErrMalformedMessage)
	}
	if len(stored.ReservationToken) == 0 {
		return wireMessage{}, fmt.Errorf("%w: empty ID reservation token", ErrMalformedMessage)
	}
	if stored.Attempt < 0 {
		return wireMessage{}, fmt.Errorf("%w: negative base attempt", ErrMalformedMessage)
	}
	return stored, nil
}

func deliveryFromWire(stored wireMessage, receiveCount int64) (*queue.Message, string, error) {
	if receiveCount <= 0 {
		return nil, "", fmt.Errorf("%w: invalid ApproximateReceiveCount %d", ErrMalformedMessage, receiveCount)
	}
	if stored.Attempt > int64(math.MaxInt)-receiveCount {
		return nil, "", fmt.Errorf("%w: attempt counter overflows int", ErrMalformedMessage)
	}
	attempt := stored.Attempt + receiveCount
	if strconv.IntSize == 32 && attempt > math.MaxInt32 {
		return nil, "", fmt.Errorf("%w: attempt counter overflows int", ErrMalformedMessage)
	}
	return &queue.Message{
		ID:                string(stored.ID),
		QueueName:         string(stored.QueueName),
		Namespace:         string(stored.Namespace),
		JobName:           string(stored.JobName),
		Payload:           cloneBytes(stored.Payload),
		Extra:             cloneBytes(stored.Extra),
		Attempt:           int(attempt),
		Delay:             0,
		VisibilityTimeout: time.Duration(stored.VisibilityNanos),
	}, string(stored.ReservationToken), nil
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	result := make([]byte, len(value))
	copy(result, value)
	return result
}
