package chunked_upload

import "sync"

type targetLock struct {
	mu   sync.Mutex
	refs int
}

var (
	targetLocksMu sync.Mutex
	targetLocks   = map[string]*targetLock{}
)

// LockTarget serializes completion for the same destination path.
func LockTarget(target string) func() {
	targetLocksMu.Lock()
	lock := targetLocks[target]
	if lock == nil {
		lock = &targetLock{}
		targetLocks[target] = lock
	}
	lock.refs++
	targetLocksMu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		targetLocksMu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(targetLocks, target)
		}
		targetLocksMu.Unlock()
	}
}
