package chunked_upload

import (
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

var (
	cleanupMu   sync.Mutex
	cleanupStop chan struct{}
)

// StartCleanup starts a background goroutine that periodically cleans up expired sessions
func StartCleanup() {
	cleanupMu.Lock()
	if cleanupStop != nil {
		cleanupMu.Unlock()
		return
	}
	cleanupStop = make(chan struct{})
	stop := cleanupStop
	cleanupMu.Unlock()
	go func() {
		ticker := time.NewTicker(CleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				GlobalSessionManager.Cleanup()
			case <-stop:
				utils.Log.Infof("[chunked_upload] cleanup goroutine stopped")
				return
			}
		}
	}()
	utils.Log.Infof("[chunked_upload] cleanup goroutine started (interval: %s, ttl: %s)", CleanupInterval, SessionTTL)
}

// StopCleanup signals the cleanup goroutine to stop gracefully
func StopCleanup() {
	cleanupMu.Lock()
	defer cleanupMu.Unlock()
	if cleanupStop != nil {
		close(cleanupStop)
		cleanupStop = nil
	}
}
