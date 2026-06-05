package chunked_upload

import (
	"time"

	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
)

// StartCleanup starts a background goroutine that periodically cleans up expired sessions
func StartCleanup() {
	go func() {
		ticker := time.NewTicker(CleanupInterval)
		defer ticker.Stop()
		for range ticker.C {
			GlobalSessionManager.Cleanup()
		}
	}()
	utils.Log.Infof("[chunked_upload] cleanup goroutine started (interval: %s, ttl: %s)", CleanupInterval, SessionTTL)
}
