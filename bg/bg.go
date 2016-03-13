package bg

import (
	"sync"
	"sync/atomic"

	"golang.org/x/net/context"
)

type CallFunc func()
type Limiter interface {
	// Enter marks the entrance to the Limiter
	// returns whether or not we should proceed
	Enter() error

	// Exit marks the exit to the Limiter
	Exit()

	Cancel()

	Done() <-chan struct{}

	// Wait until all have exited this Limiter
	Wait()

	// Call wraps the CallFunc in a limit
	Call(CallFunc) error

	Count() uint64
}

type limiter struct {
	ch        chan struct{}
	waitgroup sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	count     uint64
}

func NewLimiter(size int) Limiter {
	ctx, cancel := context.WithCancel(context.Background())
	return &limiter{
		ch:     make(chan struct{}, size),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (l *limiter) Count() uint64 {
	return l.count
}

func (l *limiter) Enter() error {
	l.waitgroup.Add(1)
	select {
	case <-l.Done():
		l.waitgroup.Done()
		return l.ctx.Err()
	case l.ch <- struct{}{}:
		atomic.AddUint64(&l.count, 1)
		return nil
	}
}

func (l *limiter) Exit() {
	atomic.AddUint64(&l.count, ^uint64(0))
	select {
	case <-l.Done():
	case <-l.ch:
		l.waitgroup.Done()
	}
}

func (l *limiter) Done() <-chan struct{} {
	return l.ctx.Done()
}

func (l *limiter) Cancel() {
	l.cancel()
}

func (l *limiter) Wait() {
	l.waitgroup.Wait()
}

func (l *limiter) Call(f CallFunc) error {
	if err := l.Enter(); err == nil {
		go func() {
			defer func() {
				l.Exit()
			}()
			f()
		}()
		return nil
	} else {
		return err
	}
}
