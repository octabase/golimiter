// SPDX-License-Identifier: Apache-2.0
// Copyright 2016 Octabase, LTD. All rights reserved.

package golimiter

type GoLimiter interface {
	Limit(limit int64)
	GetLimit() int64
	Active() int64
	Wait()
	Exec(fn func())
}

type goLimiter struct {
	limit  int64
	active int64
	cond   *sync.Cond
}

func New() GoLimiter {
	return &goLimiter{
		limit:  runtime.NumCPU(),
		active: 0,
		cond:   sync.NewCond(&sync.Mutex{}),
	}
}

func (l *goLimiter) Limit(limit int64) {
	if limit > 0 {
		atomic.StoreInt64(&l.limit, limit)
		l.cond.Broadcast()
	}
}

func (l *goLimiter) GetLimit() int64 {
	return atomic.LoadInt64(&l.limit)
}

func (l *goLimiter) Active() int64 {
	return atomic.LoadInt64(&l.active)
}

func (l *goLimiter) Wait() {
	for l.Active() > 0 {
		l.cond.L.Lock()
		l.cond.Wait()
		l.cond.L.Unlock()
	}
}

func (l *goLimiter) Exec(fn func()) {
	for {
		active := atomic.LoadInt64(&l.active)

		if active < atomic.LoadInt64(&l.limit) {
			if atomic.CompareAndSwapInt64(&l.active, active, active+1) {
				break

			} else {
				runtime.Gosched()
				continue
			}
		}

		l.cond.L.Lock()
		l.cond.Wait()
		l.cond.L.Unlock()
	}

	go func() {
		defer func() {
			atomic.AddInt64(&l.active, -1)

			l.cond.Broadcast()
		}()

		fn()
	}()
}
