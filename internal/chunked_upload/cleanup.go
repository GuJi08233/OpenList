package chunked_upload

import (
	"time"

	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

var cleanupStop chan struct{}

// StartCleanup starts a background goroutine that periodically cleans up expired sessions
func StartCleanup() {
	cleanupStop = make(chan struct{})
	go func() {
		ticker := time.NewTicker(CleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				GlobalSessionManager.Cleanup()
			case <-cleanupStop:
				utils.Log.Infof("[chunked_upload] cleanup goroutine stopped")
				return
			}
		}
	}()
	utils.Log.Infof("[chunked_upload] cleanup goroutine started (interval: %s, ttl: %s)", CleanupInterval, SessionTTL)
}

// StopCleanup signals the cleanup goroutine to stop gracefully
func StopCleanup() {
	if cleanupStop != nil {
		close(cleanupStop)
	}
}
