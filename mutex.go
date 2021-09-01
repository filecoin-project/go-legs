package legs

import "sync"

type syncMutex struct {
	isLocked bool
	lk       sync.Mutex
}

func (m *syncMutex) lock() {
	m.lk.Lock()
	m.isLocked = true
}

func (m *syncMutex) unlock() {
	if m.isLocked {
		m.lk.Unlock()
		m.isLocked = false
	}
}
