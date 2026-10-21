package stream_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

// newTestStore returns a store backed by a file in a temporary directory.
func newTestStore(t *testing.T) *stream.FileMetadataStore {
	t.Helper()
	store, err := stream.NewFileMetadataStore(filepath.Join(t.TempDir(), "cursor.jsonl"))
	if err != nil {
		t.Fatalf("NewFileMetadataStore() error = %v", err)
	}
	return store
}

// append records one checkpoint.
func appendCheckpoint(t *testing.T, store *stream.FileMetadataStore, key, cursor string, at time.Time) {
	t.Helper()
	err := store.Append(context.Background(), stream.CursorMetadata{
		Key:        key,
		Cursor:     cursor,
		Ledger:     42,
		RecordedAt: at,
	})
	if err != nil {
		t.Fatalf("Append() error = %v", err)
	}
}

func TestFileMetadataStoreRequiresAPath(t *testing.T) {
	t.Parallel()

	if _, err := stream.NewFileMetadataStore(""); err == nil {
		t.Fatal("NewFileMetadataStore() error = nil, want a missing-path failure")
	}
}

func TestFileMetadataStoreRequiresAnExistingDirectory(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(t.TempDir(), "nope", "cursor.jsonl")
	if _, err := stream.NewFileMetadataStore(missing); err == nil {
		t.Fatal("NewFileMetadataStore() error = nil, want a missing-directory failure")
	}
}

func TestFileMetadataStoreRejectsAFileAsADirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	file := filepath.Join(dir, "not-a-directory")
	if err := os.WriteFile(file, []byte("x"), 0o600); err != nil {
		t.Fatalf("writing the file: %v", err)
	}
	if _, err := stream.NewFileMetadataStore(filepath.Join(file, "cursor.jsonl")); err == nil {
		t.Fatal("NewFileMetadataStore() error = nil, want a not-a-directory failure")
	}
}

func TestFileMetadataStoreAppendAndLatest(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	now := time.Unix(1700000000, 0).UTC()
	appendCheckpoint(t, store, "events", "first", now)
	appendCheckpoint(t, store, "events", "second", now.Add(time.Second))
	appendCheckpoint(t, store, "ledgers", "other", now.Add(2*time.Second))

	latest, found, err := store.Latest(context.Background(), "events")
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	if !found {
		t.Fatal("Latest() found = false, want a checkpoint")
	}
	if latest.Cursor != "second" {
		t.Errorf("latest cursor = %q, want %q", latest.Cursor, "second")
	}
	if latest.Ledger != 42 {
		t.Errorf("latest ledger = %d, want 42", latest.Ledger)
	}
}

func TestFileMetadataStoreLatestOnAMissingFile(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	_, found, err := store.Latest(context.Background(), "events")
	if err != nil {
		t.Fatalf("Latest() error = %v, want a missing file to be empty rather than an error", err)
	}
	if found {
		t.Error("Latest() found = true, want false")
	}
}

func TestFileMetadataStoreLatestOnAMissingKey(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	appendCheckpoint(t, store, "events", "first", time.Unix(1700000000, 0).UTC())

	_, found, err := store.Latest(context.Background(), "ledgers")
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	if found {
		t.Error("Latest() found = true, want false for an unknown key")
	}
}

func TestFileMetadataStoreEntriesFiltersByKey(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	now := time.Unix(1700000000, 0).UTC()
	appendCheckpoint(t, store, "events", "a", now)
	appendCheckpoint(t, store, "ledgers", "b", now)
	appendCheckpoint(t, store, "events", "c", now)

	entries, err := store.Entries(context.Background(), "events")
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2", len(entries))
	}
	if entries[0].Cursor != "a" || entries[1].Cursor != "c" {
		t.Errorf("entries = %q, %q, want the event checkpoints in order", entries[0].Cursor, entries[1].Cursor)
	}
}

func TestFileMetadataStorePruneKeepsTheNewestPerKey(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	now := time.Unix(1700000000, 0).UTC()
	for _, cursor := range []string{"a", "b", "c", "d"} {
		appendCheckpoint(t, store, "events", cursor, now)
	}
	appendCheckpoint(t, store, "ledgers", "other", now)

	stats, err := store.Prune(context.Background(), stream.PrunePolicy{Keep: 2})
	if err != nil {
		t.Fatalf("Prune() error = %v", err)
	}
	if stats.Kept != 3 {
		t.Errorf("kept = %d, want 3 (two events and one ledger)", stats.Kept)
	}
	if stats.Removed != 2 {
		t.Errorf("removed = %d, want 2", stats.Removed)
	}

	entries, err := store.Entries(context.Background(), "events")
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("kept %d event entries, want 2", len(entries))
	}
	if entries[0].Cursor != "c" || entries[1].Cursor != "d" {
		t.Errorf("kept %q and %q, want the two newest", entries[0].Cursor, entries[1].Cursor)
	}
}

func TestFileMetadataStorePruneDropsExpiredEntries(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	now := time.Unix(1700000000, 0).UTC()
	appendCheckpoint(t, store, "events", "old", now.Add(-2*time.Hour))
	appendCheckpoint(t, store, "events", "new", now)

	stats, err := store.Prune(context.Background(), stream.PrunePolicy{Cutoff: now.Add(-time.Hour)})
	if err != nil {
		t.Fatalf("Prune() error = %v", err)
	}
	if stats.Removed != 1 || stats.Kept != 1 {
		t.Errorf("Prune() = %+v, want 1 removed and 1 kept", stats)
	}

	entries, err := store.Entries(context.Background(), "events")
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Cursor != "new" {
		t.Errorf("kept %+v, want only the fresh checkpoint", entries)
	}
}

func TestFileMetadataStorePruneOnAMissingFile(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	stats, err := store.Prune(context.Background(), stream.PrunePolicy{Keep: 1})
	if err != nil {
		t.Fatalf("Prune() error = %v, want a missing file to be empty rather than an error", err)
	}
	if stats.Kept != 0 || stats.Removed != 0 {
		t.Errorf("Prune() = %+v, want nothing to do", stats)
	}
}

func TestFileMetadataStorePruneKeepsEverythingWhenKeepIsZero(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	now := time.Unix(1700000000, 0).UTC()
	for _, cursor := range []string{"a", "b", "c"} {
		appendCheckpoint(t, store, "events", cursor, now)
	}

	stats, err := store.Prune(context.Background(), stream.PrunePolicy{})
	if err != nil {
		t.Fatalf("Prune() error = %v", err)
	}
	if stats.Removed != 0 || stats.Kept != 3 {
		t.Errorf("Prune() = %+v, want everything kept", stats)
	}
}

func TestFileMetadataStorePruneDropsUnreadableLines(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	now := time.Unix(1700000000, 0).UTC()
	appendCheckpoint(t, store, "events", "good", now)

	// A half-written line from a crashed process must not stop the pruner.
	file, err := os.OpenFile(store.Path(), os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("opening the metadata file: %v", err)
	}
	if _, err := file.WriteString("{not json\n"); err != nil {
		t.Fatalf("writing a corrupt line: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("closing the metadata file: %v", err)
	}

	stats, err := store.Prune(context.Background(), stream.PrunePolicy{Keep: 5})
	if err != nil {
		t.Fatalf("Prune() error = %v", err)
	}
	if stats.Removed != 1 {
		t.Errorf("removed = %d, want the corrupt line dropped", stats.Removed)
	}

	entries, err := store.Entries(context.Background(), "events")
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries) != 1 || entries[0].Cursor != "good" {
		t.Errorf("kept %+v, want the readable checkpoint", entries)
	}
}

func TestFileMetadataStorePruneRewritesTheFile(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	now := time.Unix(1700000000, 0).UTC()
	for _, cursor := range []string{"a", "b", "c"} {
		appendCheckpoint(t, store, "events", cursor, now)
	}

	if _, err := store.Prune(context.Background(), stream.PrunePolicy{Keep: 1}); err != nil {
		t.Fatalf("Prune() error = %v", err)
	}

	raw, err := os.ReadFile(store.Path())
	if err != nil {
		t.Fatalf("reading the metadata file: %v", err)
	}
	if lines := strings.Count(strings.TrimSpace(string(raw)), "\n") + 1; lines != 1 {
		t.Errorf("the file holds %d lines, want 1:\n%s", lines, raw)
	}
	if !strings.Contains(string(raw), `"cursor":"c"`) {
		t.Errorf("the file does not hold the newest checkpoint:\n%s", raw)
	}
}

func TestFileMetadataStorePruneKeepsTheFilePermissions(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	appendCheckpoint(t, store, "events", "a", time.Unix(1700000000, 0).UTC())
	appendCheckpoint(t, store, "events", "b", time.Unix(1700000001, 0).UTC())
	if err := os.Chmod(store.Path(), 0o640); err != nil {
		t.Fatalf("changing the file mode: %v", err)
	}

	if _, err := store.Prune(context.Background(), stream.PrunePolicy{Keep: 1}); err != nil {
		t.Fatalf("Prune() error = %v", err)
	}

	info, err := os.Stat(store.Path())
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o640 {
		t.Errorf("file mode = %o, want 640", got)
	}
}

func TestFileMetadataStoreAppendHonoursCancellation(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := store.Append(ctx, stream.CursorMetadata{Key: "events", Cursor: "a"})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Append() error = %v, want context.Canceled", err)
	}
}

// TestFileMetadataStoreAppendFailsWhenTheDirectoryIsGone checks that a
// write failure is reported rather than swallowed: a journal that silently
// stops recording is worse than an error.
func TestFileMetadataStoreAppendFailsWhenTheDirectoryIsGone(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := stream.NewFileMetadataStore(filepath.Join(dir, "cursor.jsonl"))
	if err != nil {
		t.Fatalf("NewFileMetadataStore() error = %v", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("removing the directory: %v", err)
	}

	err = store.Append(context.Background(), stream.CursorMetadata{Key: "events", Cursor: "a"})
	if err == nil {
		t.Fatal("Append() error = nil, want the write failure to be reported")
	}
	if !strings.Contains(err.Error(), "cursor metadata file") {
		t.Errorf("Append() error = %q, want it to name the file", err)
	}
}

// TestFileMetadataStorePruneFailsWhenTheFileCannotBeRead checks that a
// failed rewrite is reported rather than silently reported as "nothing to
// do".
func TestFileMetadataStorePruneFailsWhenTheFileCannotBeRead(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, "cursor.jsonl")
	store, err := stream.NewFileMetadataStore(path)
	if err != nil {
		t.Fatalf("NewFileMetadataStore() error = %v", err)
	}
	appendCheckpoint(t, store, "events", "a", time.Unix(1700000000, 0).UTC())

	// Replace the journal with a directory, which cannot be read as a file.
	if err := os.Remove(path); err != nil {
		t.Fatalf("removing the file: %v", err)
	}
	if err := os.Mkdir(path, 0o750); err != nil {
		t.Fatalf("creating the directory: %v", err)
	}

	if _, err := store.Prune(context.Background(), stream.PrunePolicy{Keep: 1}); err == nil {
		t.Fatal("Prune() error = nil, want the read failure to be reported")
	}
}

func TestFileMetadataStorePruneHonoursCancellation(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Prune(ctx, stream.PrunePolicy{}); !errors.Is(err, context.Canceled) {
		t.Errorf("Prune() error = %v, want context.Canceled", err)
	}
}

// TestFileMetadataStoreConcurrentAppendAndPrune exercises the case that
// matters in production: the stream appends a checkpoint while the pruner
// rewrites the file.
func TestFileMetadataStoreConcurrentAppendAndPrune(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	now := time.Unix(1700000000, 0).UTC()

	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for j := range 25 {
				err := store.Append(context.Background(), stream.CursorMetadata{
					Key:        "events",
					Cursor:     string(rune('a'+worker)) + string(rune('0'+j%10)),
					RecordedAt: now.Add(time.Duration(j) * time.Second),
				})
				if err != nil {
					t.Errorf("Append() error = %v", err)
					return
				}
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 25 {
			if _, err := store.Prune(context.Background(), stream.PrunePolicy{Keep: 5}); err != nil {
				t.Errorf("Prune() error = %v", err)
				return
			}
		}
	}()
	wg.Wait()

	// The pruner runs on an interval, so the file holds whatever was
	// appended since the last pass. One more pass is what bounds it.
	if _, err := store.Prune(context.Background(), stream.PrunePolicy{Keep: 5}); err != nil {
		t.Fatalf("Prune() error = %v", err)
	}

	entries, err := store.Entries(context.Background(), "events")
	if err != nil {
		t.Fatalf("Entries() error = %v", err)
	}
	if len(entries) > 5 {
		t.Errorf("kept %d entries, want at most 5", len(entries))
	}
}
