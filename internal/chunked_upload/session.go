package chunked_upload

import (
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils/random"
)

const (
	DefaultChunkSize = 50 * 1024 * 1024 // 50MB
	MinChunkSize     = 5 * 1024 * 1024  // 5MB
	MaxChunkSize     = 90 * 1024 * 1024 // 90MB (< Cloudflare 100MB limit)
	SessionTTL       = 24 * time.Hour
	CleanupInterval  = 30 * time.Minute
	UploadIDLength   = 16
)

// Session represents an in-progress chunked upload
type Session struct {
	UploadID     string `json:"upload_id"`
	UserID       uint   `json:"user_id"` // owner user ID for ownership validation
	FileName     string `json:"file_name"`
	FilePath     string `json:"file_path"` // destination path (after JoinPath validation)
	FileSize     int64  `json:"file_size"`
	ChunkSize    int64  `json:"chunk_size"`
	TotalChunks  int    `json:"total_chunks"`
	Overwrite    bool   `json:"overwrite"`
	MimeType     string `json:"mime_type"`
	LastModified int64  `json:"last_modified"` // millisecond timestamp
	CreatedAt    int64  `json:"created_at"`    // unix timestamp
	ExpiresAt    int64  `json:"expires_at"`    // unix timestamp

	// Hash values for rapid upload / deduplication (optional)
	HashMd5    string `json:"hash_md5"`
	HashSha1   string `json:"hash_sha1"`
	HashSha256 string `json:"hash_sha256"`

	mu             sync.Mutex
	uploadedChunks map[int]bool
	chunkLocks     sync.Map // map[int]*sync.Mutex — per-chunk lock for concurrent uploads
	completing     bool
	completed      bool
}

// GetChunkLock returns the mutex for a specific chunk index, creating it if needed.
func (s *Session) GetChunkLock(index int) *sync.Mutex {
	lockVal, _ := s.chunkLocks.LoadOrStore(index, &sync.Mutex{})
	return lockVal.(*sync.Mutex)
}

// IsChunkUploaded checks if a specific chunk has been uploaded (without locking uploadedChunks).
// This is used after acquiring the per-chunk lock to skip redundant writes.
func (s *Session) IsChunkUploaded(index int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.uploadedChunks[index]
}

// MarkChunkUploaded marks a chunk as uploaded.
// Returns false if the chunk was already marked (prevents concurrent duplicate writes).
func (s *Session) MarkChunkUploaded(index int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.uploadedChunks[index] {
		return false
	}
	s.uploadedChunks[index] = true
	return true
}

// GetUploadedChunks returns a sorted list of uploaded chunk indices
func (s *Session) GetUploadedChunks() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]int, 0, len(s.uploadedChunks))
	for idx := range s.uploadedChunks {
		result = append(result, idx)
	}
	sort.Ints(result)
	return result
}

// UploadedCount returns the number of uploaded chunks
func (s *Session) UploadedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.uploadedChunks)
}

// IsComplete returns true if all chunks have been uploaded
func (s *Session) IsComplete() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.uploadedChunks) == s.TotalChunks
}

// BeginComplete marks the session as being completed.
// It returns false when another complete request is already in progress or done.
func (s *Session) BeginComplete() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completing || s.completed {
		return false
	}
	s.completing = true
	return true
}

// FinishComplete clears the completing flag and records successful completion.
func (s *Session) FinishComplete(success bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completing = false
	if success {
		s.completed = true
	}
}

// IsCompleting reports whether the session is currently being completed.
func (s *Session) IsCompleting() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.completing
}

// ExpectedChunkSize returns the exact byte length required for a chunk index.
func (s *Session) ExpectedChunkSize(index int) (int64, bool) {
	if index < 0 || index >= s.TotalChunks {
		return 0, false
	}
	if index == s.TotalChunks-1 {
		size := s.FileSize - int64(index)*s.ChunkSize
		return size, size > 0
	}
	return s.ChunkSize, true
}

// IsExpired returns true if the session has expired
func (s *Session) IsExpired() bool {
	return time.Now().Unix() > s.ExpiresAt
}

// SessionManager manages all active chunked upload sessions
type SessionManager struct {
	sessions sync.Map // map[string]*Session
}

// GlobalSessionManager is the global session manager instance
var GlobalSessionManager = &SessionManager{}

// Create creates a new chunked upload session
func (m *SessionManager) Create(userID uint, fileName, filePath string, fileSize, chunkSize int64, overwrite bool, mimeType string, lastModified int64, md5, sha1, sha256 string) *Session {
	totalChunks := int(math.Ceil(float64(fileSize) / float64(chunkSize)))
	now := time.Now()
	s := &Session{
		UploadID:       GenerateUploadID(),
		UserID:         userID,
		FileName:       fileName,
		FilePath:       filePath,
		FileSize:       fileSize,
		ChunkSize:      chunkSize,
		TotalChunks:    totalChunks,
		Overwrite:      overwrite,
		MimeType:       mimeType,
		LastModified:   lastModified,
		CreatedAt:      now.Unix(),
		ExpiresAt:      now.Add(SessionTTL).Unix(),
		HashMd5:        md5,
		HashSha1:       sha1,
		HashSha256:     sha256,
		uploadedChunks: make(map[int]bool),
	}
	m.sessions.Store(s.UploadID, s)
	return s
}

// Get retrieves a session by upload ID
func (m *SessionManager) Get(uploadID string) (*Session, bool) {
	if !IsValidUploadID(uploadID) {
		return nil, false
	}
	val, ok := m.sessions.Load(uploadID)
	if !ok {
		return nil, false
	}
	return val.(*Session), true
}

// Delete removes a session and its chunk files from disk
func (m *SessionManager) Delete(uploadID string) {
	if !IsValidUploadID(uploadID) {
		return
	}
	m.sessions.Delete(uploadID)
	os.RemoveAll(ChunkDir(uploadID))
}

// Cleanup removes all expired sessions and their chunk files
func (m *SessionManager) Cleanup() {
	now := time.Now().Unix()
	m.sessions.Range(func(key, value any) bool {
		session := value.(*Session)
		if now > session.ExpiresAt && !session.IsCompleting() {
			os.RemoveAll(ChunkDir(session.UploadID))
			m.sessions.Delete(key)
		}
		return true
	})
}

// GenerateUploadID creates a random upload ID
func GenerateUploadID() string {
	return random.String(UploadIDLength)
}

// IsValidUploadID checks whether an upload ID can refer to a managed chunk dir.
func IsValidUploadID(uploadID string) bool {
	if len(uploadID) != UploadIDLength {
		return false
	}
	for _, ch := range uploadID {
		if (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') {
			continue
		}
		return false
	}
	return true
}

// ChunkDir returns the filesystem path for chunk storage
func ChunkDir(uploadID string) string {
	return filepath.Join(conf.Conf.TempDir, "chunks", uploadID)
}

// ChunkPath returns the filesystem path for a specific chunk
func ChunkPath(uploadID string, chunkIndex int) string {
	return filepath.Join(ChunkDir(uploadID), "chunk_"+strconv.Itoa(chunkIndex))
}

// CalcTotalChunks calculates the total number of chunks
func CalcTotalChunks(fileSize, chunkSize int64) int {
	return int(math.Ceil(float64(fileSize) / float64(chunkSize)))
}
