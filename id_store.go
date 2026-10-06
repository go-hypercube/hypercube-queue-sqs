package sqsqueue

import (
	"context"
	"crypto/rand"
	"errors"
	"sync"
)

// ErrReservationOwnershipLost means an ID reservation still exists but is no
// longer owned by the token supplied to Release. Implementations must never
// remove a reservation owned by another token.
var ErrReservationOwnershipLost = errors.New("sqs queue: message ID reservation ownership lost")

// IDStore atomically reserves logical message IDs across the isolation scope
// chosen by its implementation. A reservation remains held while the logical
// message is ready, delayed, in flight, or dead-lettered.
//
// Reserve returns reserved=false when id is already held. token is persisted in
// the SQS message and passed back to Release after a confirmed Ack. Release must
// compare the token before deleting so delayed cleanup cannot release a newer
// reservation for the same ID. Release should be idempotent when no reservation
// exists, which allows consumers to acknowledge messages after an ID store was
// rebuilt or intentionally expired.
//
// A distributed implementation (for example, a conditional DynamoDB item) is
// required when producers in multiple processes must share strict ID uniqueness.
type IDStore interface {
	Reserve(ctx context.Context, id string) (token string, reserved bool, err error)
	Release(ctx context.Context, id, token string) error
}

// MemoryIDStore is a concurrent process-local IDStore. It enforces uniqueness
// only among Queue instances configured with the same MemoryIDStore value and
// loses reservations when the process exits.
type MemoryIDStore struct {
	mu           sync.Mutex
	reservations map[string]string
}

// NewMemoryIDStore creates an empty process-local ID registry.
func NewMemoryIDStore() *MemoryIDStore {
	return &MemoryIDStore{reservations: make(map[string]string)}
}

// Reserve atomically reserves id when it is not already held.
func (s *MemoryIDStore) Reserve(ctx context.Context, id string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	token := rand.Text()

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	if _, exists := s.reservations[id]; exists {
		return "", false, nil
	}
	if s.reservations == nil {
		s.reservations = make(map[string]string)
	}
	s.reservations[id] = token
	return token, true, nil
}

// Release removes id only when token still owns its reservation.
func (s *MemoryIDStore) Release(ctx context.Context, id, token string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	current, exists := s.reservations[id]
	if !exists {
		return nil
	}
	if current != token {
		return ErrReservationOwnershipLost
	}
	delete(s.reservations, id)
	return nil
}
