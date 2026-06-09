package chunked_upload

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
)

func withTempConfig(t *testing.T) {
	t.Helper()
	oldConf := conf.Conf
	conf.Conf = conf.DefaultConfig(t.TempDir())
	t.Cleanup(func() {
		conf.Conf = oldConf
	})
}

func TestIsValidUploadID(t *testing.T) {
	tests := []struct {
		name string
		id   string
		want bool
	}{
		{name: "generated shape", id: "AbCdEfGh12345678", want: true},
		{name: "too short", id: "abc", want: false},
		{name: "path separator", id: "AbCdEfGh1234/678", want: false},
		{name: "dot segment", id: "AbCdEfGh1234..78", want: false},
		{name: "dash", id: "AbCdEfGh1234-678", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsValidUploadID(tt.id); got != tt.want {
				t.Fatalf("IsValidUploadID(%q) = %v, want %v", tt.id, got, tt.want)
			}
		})
	}
}

func TestDeleteIgnoresInvalidUploadID(t *testing.T) {
	withTempConfig(t)

	victim := filepath.Join(conf.Conf.TempDir, "victim")
	if err := os.MkdirAll(victim, 0700); err != nil {
		t.Fatal(err)
	}

	GlobalSessionManager.Delete("../victim")

	if _, err := os.Stat(victim); err != nil {
		t.Fatalf("invalid upload id removed unrelated directory: %v", err)
	}
}

func TestExpectedChunkSize(t *testing.T) {
	session := &Session{
		FileSize:    11,
		ChunkSize:   5,
		TotalChunks: 3,
	}

	tests := []struct {
		index int
		size  int64
		ok    bool
	}{
		{index: -1, ok: false},
		{index: 0, size: 5, ok: true},
		{index: 1, size: 5, ok: true},
		{index: 2, size: 1, ok: true},
		{index: 3, ok: false},
	}

	for _, tt := range tests {
		got, ok := session.ExpectedChunkSize(tt.index)
		if ok != tt.ok || got != tt.size {
			t.Fatalf("ExpectedChunkSize(%d) = (%d, %v), want (%d, %v)", tt.index, got, ok, tt.size, tt.ok)
		}
	}
}

func TestMergeChunksReadsSequentiallyAndValidatesSize(t *testing.T) {
	withTempConfig(t)

	session := &Session{
		UploadID:    "AbCdEfGh12345678",
		FileSize:    7,
		ChunkSize:   3,
		TotalChunks: 3,
	}
	if err := os.MkdirAll(ChunkDir(session.UploadID), 0700); err != nil {
		t.Fatal(err)
	}
	chunks := [][]byte{[]byte("abc"), []byte("def"), []byte("g")}
	for i, chunk := range chunks {
		if err := os.WriteFile(ChunkPath(session.UploadID, i), chunk, 0600); err != nil {
			t.Fatal(err)
		}
	}

	reader, cleanup, err := MergeChunks(session)
	if err != nil {
		t.Fatalf("MergeChunks returned error: %v", err)
	}
	got, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll returned error: %v", err)
	}
	if string(got) != "abcdefg" {
		t.Fatalf("merged content = %q, want %q", string(got), "abcdefg")
	}
	if err := cleanup(); err != nil {
		t.Fatalf("cleanup returned error: %v", err)
	}

	if err := os.WriteFile(ChunkPath(session.UploadID, 2), []byte("too long"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := MergeChunks(session); err == nil {
		t.Fatal("MergeChunks accepted a chunk with invalid size")
	}
}

func TestCleanupStartStopIsIdempotent(t *testing.T) {
	StartCleanup()
	StopCleanup()
	StopCleanup()
	StartCleanup()
	StopCleanup()
}
