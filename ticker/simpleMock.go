package ticker

import (
	"errors"
	"sync/atomic"
	"time"
)

var (
	// ErrTimeout happens when the force tick cannot be delivered in time.
	ErrTimeout = errors.New("force tick timeout")

	// ErrPaused happens when the ticker is paused and the caller still
	// wants to force a tick.
	ErrPaused = errors.New("cannot force feed because ticker is paused")
)

// SimpleMock implements the Ticker interface, and provides a method of
// force-feeding ticks, even while paused.
type SimpleMock struct {
	isActive uint32 // used atomically

	// Force is used to force-feed a ticks into the ticker. Useful for
	// debugging when trying to wake an event.
	force chan time.Time
}

// NewSimpleMock returns a SimpleMock Ticker, used for testing and debugging. It
// supports the ability to force-feed events that get output by the
func NewSimpleMock() *SimpleMock {
	m := &SimpleMock{
		force: make(chan time.Time),
	}
	return m
}

// Ticks returns a receive-only channel that delivers times at the ticker's
// prescribed interval when active. Force-fed ticks can be delivered at any
// time.
//
// NOTE: Part of the Ticker interface.
func (m *SimpleMock) Ticks() <-chan time.Time {
	return m.force
}

// Resume starts underlying time.Ticker and causes the ticker to begin
// delivering scheduled events.
//
// NOTE: Part of the Ticker interface.
func (m *SimpleMock) Resume() {
	atomic.StoreUint32(&m.isActive, 1)
}

// Pause suspends the underlying ticker, such that Ticks() stops signaling at
// regular intervals.
//
// NOTE: Part of the Ticker interface.
func (m *SimpleMock) Pause() {
	atomic.StoreUint32(&m.isActive, 0)
}

// Stop suspends the underlying ticker, such that Ticks() stops signaling at
// regular intervals, and permanently frees up any resources.
//
// NOTE: Part of the Ticker interface.
func (m *SimpleMock) Stop() {
	atomic.StoreUint32(&m.isActive, 0)
}

// IsActive returns the current active state of the ticker.
func (m *SimpleMock) IsActive() bool {
	return atomic.LoadUint32(&m.isActive) == 1
}

// ForceTick forces a tick to be sent out by Ticker. It returns an error if the
// ticker is paused, to prevent testing scenarios that can never happen in
// production.
func (m *SimpleMock) ForceTick(timeStamp time.Time, timeout time.Duration) error {
	if !m.IsActive() {
		return ErrPaused
	}

	select {
	case m.force <- timeStamp:
	case <-time.After(timeout):
		return ErrTimeout
	}

	return nil
}
