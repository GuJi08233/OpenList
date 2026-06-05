package chunked_upload

import (
	"io"
	"os"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

// MergeChunks opens all chunk files in order and returns an io.Reader
// that reads them sequentially, plus a cleanup function.
func MergeChunks(session *Session) (io.Reader, func() error, error) {
	readers := make([]io.Reader, 0, session.TotalChunks)
	closers := make([]io.Closer, 0, session.TotalChunks)

	for i := 0; i < session.TotalChunks; i++ {
		f, err := os.Open(ChunkPath(session.UploadID, i))
		if err != nil {
			for _, c := range closers {
				c.Close()
			}
			return nil, nil, err
		}
		readers = append(readers, f)
		closers = append(closers, f)
	}

	merged := io.MultiReader(readers...)
	cleanup := func() error {
		for _, c := range closers {
			c.Close()
		}
		os.RemoveAll(ChunkDir(session.UploadID))
		return nil
	}
	return merged, cleanup, nil
}

// BuildFileStream creates a FileStream from a merged session
func BuildFileStream(session *Session, reader io.Reader, cleanup func() error) *stream.FileStream {
	lastModified := time.Now()
	if session.LastModified > 0 {
		lastModified = time.UnixMilli(session.LastModified)
	}
	fs := &stream.FileStream{
		Obj: &model.Object{
			Name:     session.FileName,
			Size:     session.FileSize,
			Modified: lastModified,
		},
		Reader:   reader,
		Mimetype: session.MimeType,
	}
	if cleanup != nil {
		fs.Closers.Add(utils.CloseFunc(cleanup))
	}
	return fs
}
