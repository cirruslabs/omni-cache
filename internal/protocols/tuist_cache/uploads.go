package tuist_cache

import (
	"errors"
	"sync"
	"time"

	"github.com/cirruslabs/omni-cache/pkg/storage"
	"github.com/google/uuid"
)

var (
	errUploadNotFound = errors.New("upload not found")
	errPartsMismatch  = errors.New("parts mismatch")
)

type uploadStore struct {
	mu       sync.Mutex
	now      func() time.Time
	ttl      time.Duration
	sessions map[string]*uploadSession
}

type uploadSession struct {
	// Tuist /complete only provides part numbers, so we must keep backend upload
	// identity and per-part ETags server-side to finish S3 multipart commit.
	key             string
	backendUploadID string
	parts           map[int]storage.MultipartUploadPart
	lastTouchedAt   time.Time
}

type completedUpload struct {
	key             string
	backendUploadID string
	parts           []storage.MultipartUploadPart
}

func newUploadStore(now func() time.Time, ttl time.Duration) *uploadStore {
	if now == nil {
		now = time.Now
	}
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}

	return &uploadStore{
		now:      now,
		ttl:      ttl,
		sessions: map[string]*uploadSession{},
	}
}

func (s *uploadStore) create(key string, backendUploadID string) string {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupExpired()

	uploadID := uuid.NewString()
	s.sessions[uploadID] = &uploadSession{
		key:             key,
		backendUploadID: backendUploadID,
		parts:           map[int]storage.MultipartUploadPart{},
		lastTouchedAt:   s.now(),
	}

	return uploadID
}

func (s *uploadStore) preparePart(uploadID string, partSize int64) (key string, backendUploadID string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupExpired()

	session, ok := s.sessions[uploadID]
	if !ok {
		return "", "", errUploadNotFound
	}

	if partSize > maxPartSizeBytes {
		return "", "", errPartTooLarge
	}
	session.lastTouchedAt = s.now()

	return session.key, session.backendUploadID, nil
}

func (s *uploadStore) setPart(uploadID string, partNumber int, etag string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupExpired()

	session, ok := s.sessions[uploadID]
	if !ok {
		return errUploadNotFound
	}

	session.parts[partNumber] = storage.MultipartUploadPart{
		PartNumber: uint32(partNumber),
		ETag:       etag,
	}
	session.lastTouchedAt = s.now()
	return nil
}

func (s *uploadStore) complete(uploadID string, requestedParts []int) (*completedUpload, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.cleanupExpired()

	session, ok := s.sessions[uploadID]
	if !ok {
		return nil, errUploadNotFound
	}

	serverParts := make([]int, 0, len(session.parts))
	for partNumber := range session.parts {
		serverParts = append(serverParts, partNumber)
	}

	if !equalPartNumbers(serverParts, requestedParts) {
		return nil, errPartsMismatch
	}

	parts := make([]storage.MultipartUploadPart, 0, len(requestedParts))
	for _, partNumber := range requestedParts {
		part, ok := session.parts[partNumber]
		if !ok {
			return nil, errPartsMismatch
		}
		parts = append(parts, part)
	}

	session.lastTouchedAt = s.now()

	return &completedUpload{
		key:             session.key,
		backendUploadID: session.backendUploadID,
		parts:           parts,
	}, nil
}

func (s *uploadStore) finalize(uploadID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.sessions, uploadID)
}

func (s *uploadStore) cleanupExpired() {
	now := s.now()

	for uploadID, session := range s.sessions {
		if now.Sub(session.lastTouchedAt) > s.ttl {
			delete(s.sessions, uploadID)
		}
	}
}
