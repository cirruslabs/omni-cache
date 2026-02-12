package tuist_cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestUploadStoreRetainsSessionUntilFinalize(t *testing.T) {
	now := time.Unix(0, 0)
	store := newUploadStore(func() time.Time { return now }, 5*time.Minute)

	uploadID := store.create("key", "backend-upload")
	require.NoError(t, store.setPart(uploadID, 1, "etag-1"))

	completion, err := store.complete(uploadID, []int{1})
	require.NoError(t, err)
	require.NotNil(t, completion)

	_, _, err = store.preparePart(uploadID, 1)
	require.NoError(t, err)

	store.finalize(uploadID)

	_, _, err = store.preparePart(uploadID, 1)
	require.ErrorIs(t, err, errUploadNotFound)
}

func TestUploadStoreRefreshesTTLOnActivity(t *testing.T) {
	now := time.Unix(0, 0)
	store := newUploadStore(func() time.Time { return now }, 5*time.Minute)

	uploadID := store.create("key", "backend-upload")

	now = now.Add(4 * time.Minute)
	_, _, err := store.preparePart(uploadID, 1)
	require.NoError(t, err)

	now = now.Add(4 * time.Minute)
	require.NoError(t, store.setPart(uploadID, 1, "etag-1"))

	now = now.Add(4 * time.Minute)
	_, _, err = store.preparePart(uploadID, 1)
	require.NoError(t, err)

	now = now.Add(6 * time.Minute)
	_, _, err = store.preparePart(uploadID, 1)
	require.ErrorIs(t, err, errUploadNotFound)
}
