package chunked_upload

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

// MergeChunks opens all chunk files in order and returns an io.Reader
// that reads them sequentially, plus a cleanup function that closes all file handles.
// Note: directory removal is handled separately by SessionManager.Delete.
func MergeChunks(session *Session) (io.Reader, func() error, error) {
	readers := make([]io.Reader, 0, session.TotalChunks)
	closers := make([]io.Closer, 0, session.TotalChunks)

	for i := 0; i < session.TotalChunks; i++ {
		f, err := os.Open(ChunkPath(session.UploadID, i))
		if err != nil {
			// Close any files already opened before returning error
			for _, c := range closers {
				c.Close()
			}
			return nil, nil, fmt.Errorf("failed to open chunk %d: %w", i, err)
		}
		readers = append(readers, f)
		closers = append(closers, f)
	}

	merged := io.MultiReader(readers...)
	cleanup := func() error {
		var firstErr error
		for _, c := range closers {
			if err := c.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		return firstErr
	}
	return merged, cleanup, nil
}

// BuildFileStream creates a FileStream from a merged session.
// hashInfo may be nil if no hash values are provided.
func BuildFileStream(session *Session, reader io.Reader, cleanup func() error, hashInfo utils.HashInfo) *stream.FileStream {
	lastModified := time.Now()
	if session.LastModified > 0 {
		lastModified = time.UnixMilli(session.LastModified)
	}
	fs := &stream.FileStream{
		Obj: &model.Object{
			Name:     session.FileName,
			Size:     session.FileSize,
			Modified: lastModified,
			HashInfo: hashInfo,
		},
		Reader:   reader,
		Mimetype: session.MimeType,
	}
	if cleanup != nil {
		fs.Closers.Add(utils.CloseFunc(cleanup))
	}
	return fs
}
