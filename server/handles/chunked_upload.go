package handles

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path"
	"strconv"

	"github.com/OpenListTeam/OpenList/v4/internal/chunked_upload"
	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/setting"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
)

type ChunkedCreateReq struct {
	Size         int64  `json:"size"`
	ChunkSize    int64  `json:"chunk_size"`
	Name         string `json:"name"`
	MimeType     string `json:"mime_type"`
	LastModified int64  `json:"last_modified"` // millisecond timestamp
	// Optional hash values for rapid upload / deduplication
	Md5    string `json:"md5"`
	Sha1   string `json:"sha1"`
	Sha256 string `json:"sha256"`
}

type ChunkedCompleteReq struct {
	UploadID string `json:"upload_id"`
	AsTask   bool   `json:"as_task"`
}

type ChunkedAbortReq struct {
	UploadID string `json:"upload_id"`
}

// ChunkedUploadCreate initializes a chunked upload session
func ChunkedUploadCreate(c *gin.Context) {
	// Check if chunked upload is enabled
	if !setting.GetBool(conf.EnableChunkedUpload) {
		common.ErrorStrResp(c, "chunked upload is not enabled", 403)
		return
	}

	var req ChunkedCreateReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}

	// Parse and validate destination path (same as FsStream)
	filePath := c.GetHeader("File-Path")
	filePath, err := url.PathUnescape(filePath)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	filePath, err = user.JoinPath(filePath)
	if err != nil {
		common.ErrorResp(c, err, 403)
		return
	}

	// Validate file name
	if req.Name == "" {
		req.Name = path.Base(filePath)
	}

	// Check if system file should be ignored (same as FsStream/FsForm)
	if setting.GetBool(conf.IgnoreSystemFiles) && utils.IsSystemFile(req.Name) {
		common.ErrorStrResp(c, errs.IgnoredSystemFile.Error(), 403)
		return
	}

	// Check overwrite
	overwrite := c.GetHeader("Overwrite") != "false"
	if !overwrite {
		if res, _ := fs.Get(c.Request.Context(), filePath, &fs.GetArgs{NoLog: true}); res != nil {
			common.ErrorStrResp(c, "file exists", 403)
			return
		}
	}

	// Validate file size
	if req.Size <= 0 {
		common.ErrorStrResp(c, "invalid file size", 400)
		return
	}

	// Validate and normalize chunk size
	chunkSize := req.ChunkSize
	if chunkSize <= 0 {
		// Use configured chunk size from settings (in MB), convert to bytes
		configuredSize := setting.GetInt(conf.ChunkedUploadSize, 50)
		chunkSize = int64(configuredSize) * 1024 * 1024
	}
	if chunkSize < chunked_upload.MinChunkSize {
		chunkSize = chunked_upload.MinChunkSize
	}
	if chunkSize > chunked_upload.MaxChunkSize {
		chunkSize = chunked_upload.MaxChunkSize
	}

	// Mimetype fallback: derive from file name if not provided (same as FsStream)
	mimeType := req.MimeType
	if mimeType == "" {
		mimeType = utils.GetMimeType(req.Name)
	}

	// Create session with user ownership
	session := chunked_upload.GlobalSessionManager.Create(
		user.ID, req.Name, filePath, req.Size, chunkSize, mimeType, req.LastModified,
		req.Md5, req.Sha1, req.Sha256,
	)

	// Create chunk directory
	chunkDir := chunked_upload.ChunkDir(session.UploadID)
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		chunked_upload.GlobalSessionManager.Delete(session.UploadID)
		common.ErrorResp(c, fmt.Errorf("failed to create chunk directory: %w", err), 500)
		return
	}

	common.SuccessResp(c, gin.H{
		"upload_id":    session.UploadID,
		"chunk_size":   session.ChunkSize,
		"total_chunks": session.TotalChunks,
		"expires_at":   session.ExpiresAt,
	})
}

// ChunkedUploadChunk uploads a single chunk
func ChunkedUploadChunk(c *gin.Context) {
	uploadID := c.GetHeader("Upload-Id")
	chunkIndexStr := c.GetHeader("Chunk-Index")

	if uploadID == "" || chunkIndexStr == "" {
		common.ErrorStrResp(c, "missing Upload-Id or Chunk-Index header", 400)
		return
	}

	chunkIndex, err := strconv.Atoi(chunkIndexStr)
	if err != nil {
		common.ErrorStrResp(c, "invalid Chunk-Index", 400)
		return
	}

	session, ok := chunked_upload.GlobalSessionManager.Get(uploadID)
	if !ok {
		common.ErrorStrResp(c, "upload session not found", 404)
		return
	}

	if session.IsExpired() {
		chunked_upload.GlobalSessionManager.Delete(uploadID)
		common.ErrorStrResp(c, "upload session expired", 410)
		return
	}

	// Verify user ownership: only the session creator (or admin) can upload chunks
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	if user.ID != session.UserID && !user.IsAdmin() {
		common.ErrorStrResp(c, "permission denied", 403)
		return
	}

	if chunkIndex < 0 || chunkIndex >= session.TotalChunks {
		common.ErrorStrResp(c, fmt.Sprintf("chunk index out of range [0, %d)", session.TotalChunks), 400)
		return
	}

	// Write chunk data to file, limiting read size to prevent disk exhaustion.
	// Allow a small margin (4KB) over the expected chunk size for encoding overhead.
	chunkPath := chunked_upload.ChunkPath(uploadID, chunkIndex)
	out, err := os.Create(chunkPath)
	if err != nil {
		common.ErrorResp(c, fmt.Errorf("failed to create chunk file: %w", err), 500)
		return
	}
	defer out.Close()

	limit := session.ChunkSize + 4096
	limitedReader := io.LimitReader(c.Request.Body, limit)

	written, err := io.Copy(out, limitedReader)
	if err != nil {
		os.Remove(chunkPath)
		common.ErrorResp(c, fmt.Errorf("failed to write chunk data: %w", err), 500)
		return
	}

	// Mark as uploaded only after successful write.
	// If this chunk was already uploaded (concurrent retry), the second mark is a no-op.
	session.MarkChunkUploaded(chunkIndex)

	common.SuccessResp(c, gin.H{
		"chunk_index":     chunkIndex,
		"chunk_size":      written,
		"uploaded_chunks": session.UploadedCount(),
		"total_chunks":    session.TotalChunks,
	})
}

// ChunkedUploadComplete merges all chunks and writes to storage
func ChunkedUploadComplete(c *gin.Context) {
	var req ChunkedCompleteReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}

	if req.UploadID == "" {
		common.ErrorStrResp(c, "missing upload_id", 400)
		return
	}

	session, ok := chunked_upload.GlobalSessionManager.Get(req.UploadID)
	if !ok {
		common.ErrorStrResp(c, "upload session not found", 404)
		return
	}

	// Verify user ownership: only the session creator (or admin) can complete the upload
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	if user.ID != session.UserID && !user.IsAdmin() {
		common.ErrorStrResp(c, "permission denied", 403)
		return
	}

	if !session.IsComplete() {
		common.ErrorStrResp(c, fmt.Sprintf("not all chunks uploaded: %d/%d",
			session.UploadedCount(), session.TotalChunks), 400)
		return
	}

	// Build hash info from session (for rapid upload / deduplication if provided)
	hashMap := make(map[*utils.HashType]string)
	if session.HashMd5 != "" {
		hashMap[utils.MD5] = session.HashMd5
	}
	if session.HashSha1 != "" {
		hashMap[utils.SHA1] = session.HashSha1
	}
	if session.HashSha256 != "" {
		hashMap[utils.SHA256] = session.HashSha256
	}
	hashInfo := utils.NewHashInfoByMap(hashMap)

	// Merge chunks into a single reader
	mergedReader, cleanup, err := chunked_upload.MergeChunks(session)
	if err != nil {
		common.ErrorResp(c, fmt.Errorf("failed to merge chunks: %w", err), 500)
		return
	}

	// Build FileStream
	fileStream := chunked_upload.BuildFileStream(session, mergedReader, cleanup, hashInfo)
	fileStream.WebPutAsTask = req.AsTask

	dir, _ := path.Split(session.FilePath)

	// Use the same upload flow as FsStream
	if req.AsTask {
		taskInfo, putErr := fs.PutAsTask(c.Request.Context(), dir, fileStream)
		if putErr != nil {
			chunked_upload.GlobalSessionManager.Delete(req.UploadID)
			common.ErrorResp(c, putErr, 500)
			return
		}
		chunked_upload.GlobalSessionManager.Delete(req.UploadID)
		common.SuccessResp(c, gin.H{
			"task": getTaskInfo(taskInfo),
		})
	} else {
		putErr := fs.PutDirectly(c.Request.Context(), dir, fileStream)
		if putErr != nil {
			chunked_upload.GlobalSessionManager.Delete(req.UploadID)
			common.ErrorResp(c, putErr, 500)
			return
		}
		chunked_upload.GlobalSessionManager.Delete(req.UploadID)
		common.SuccessResp(c)
	}
}

// ChunkedUploadStatus returns the status of a chunked upload session
func ChunkedUploadStatus(c *gin.Context) {
	uploadID := c.Query("upload_id")
	if uploadID == "" {
		common.ErrorStrResp(c, "missing upload_id parameter", 400)
		return
	}

	session, ok := chunked_upload.GlobalSessionManager.Get(uploadID)
	if !ok {
		common.ErrorStrResp(c, "upload session not found", 404)
		return
	}

	uploadedChunks := session.GetUploadedChunks()

	common.SuccessResp(c, gin.H{
		"upload_id":       session.UploadID,
		"file_name":       session.FileName,
		"file_size":       session.FileSize,
		"chunk_size":      session.ChunkSize,
		"total_chunks":    session.TotalChunks,
		"uploaded_chunks": uploadedChunks,
		"created_at":      session.CreatedAt,
		"expires_at":      session.ExpiresAt,
	})
}

// ChunkedUploadAbort cancels a chunked upload and cleans up
func ChunkedUploadAbort(c *gin.Context) {
	var req ChunkedAbortReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}

	if req.UploadID == "" {
		common.ErrorStrResp(c, "missing upload_id", 400)
		return
	}

	// Verify user ownership before aborting
	if session, ok := chunked_upload.GlobalSessionManager.Get(req.UploadID); ok {
		user := c.Request.Context().Value(conf.UserKey).(*model.User)
		if user.ID != session.UserID && !user.IsAdmin() {
			common.ErrorStrResp(c, "permission denied", 403)
			return
		}
	}

	chunked_upload.GlobalSessionManager.Delete(req.UploadID)
	common.SuccessResp(c)
}
