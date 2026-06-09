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

type sequentialChunkReader struct {
	session *Session
	index   int
	current *os.File
	closed  bool
}

func (r *sequentialChunkReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.ErrClosedPipe
	}
	for {
		if r.current == nil {
			if r.index >= r.session.TotalChunks {
				return 0, io.EOF
			}
			f, err := os.Open(ChunkPath(r.session.UploadID, r.index))
			if err != nil {
				return 0, fmt.Errorf("failed to open chunk %d: %w", r.index, err)
			}
			r.current = f
		}

		n, err := r.current.Read(p)
		if err == io.EOF {
			closeErr := r.current.Close()
			r.current = nil
			r.index++
			if n > 0 {
				if closeErr != nil {
					return n, closeErr
				}
				return n, nil
			}
			if closeErr != nil {
				return 0, closeErr
			}
			continue
		}
		return n, err
	}
}

func (r *sequentialChunkReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.current != nil {
		err := r.current.Close()
		r.current = nil
		return err
	}
	return nil
}

// MergeChunks opens all chunk files in order and returns an io.Reader
// that reads them sequentially, plus a cleanup function that closes all file handles.
// Note: directory removal is handled separately by SessionManager.Delete.
func MergeChunks(session *Session) (io.Reader, func() error, error) {
	for i := 0; i < session.TotalChunks; i++ {
		info, err := os.Stat(ChunkPath(session.UploadID, i))
		if err != nil {
			return nil, nil, fmt.Errorf("failed to open chunk %d: %w", i, err)
		}
		expectedSize, ok := session.ExpectedChunkSize(i)
		if !ok {
			return nil, nil, fmt.Errorf("invalid chunk index %d", i)
		}
		if info.Size() != expectedSize {
			return nil, nil, fmt.Errorf("invalid chunk %d size: expected %d, got %d", i, expectedSize, info.Size())
		}
	}

	merged := &sequentialChunkReader{session: session}
	return merged, merged.Close, nil
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
