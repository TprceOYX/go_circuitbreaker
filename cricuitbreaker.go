package main

import (
	"errors"
	"sync/atomic"
	"time"
)

const (
	StateClosed   uint32 = 1
	StateHalfOpen uint32 = 2
	StateOpen     uint32 = 3
)

type statistic struct {
	requests            uint32
	continuousSuccesses uint32
	continuousFailures  uint32
}

func (s *statistic) request() {
	s.requests++
}

func (s *statistic) success() uint32 {
	s.request()
	atomic.CompareAndSwapUint32(&s.continuousFailures, s.continuousFailures, 0)
	return atomic.AddUint32(&s.continuousSuccesses, 1)
}

func (s *statistic) fail() uint32 {
	s.request()
	atomic.CompareAndSwapUint32(&s.continuousSuccesses, s.continuousSuccesses, 0)
	return atomic.AddUint32(&s.continuousFailures, 1)
}

type CricuitBreaker struct {
	s *statistic

	state        uint32
	openInterval int64
	openExpire   int64
	threshold    uint32
	cycle        uint64
}

func NewCricuitBreaker(openInterval int64, threshold uint32) *CricuitBreaker {
	if openInterval <= 0 {
		openInterval = 60
	}
	if threshold <= 0 {
		threshold = 5
	}
	return &CricuitBreaker{
		s: &statistic{
			requests:            0,
			continuousSuccesses: 0,
			continuousFailures:  0,
		},
		state:        StateClosed,
		openInterval: openInterval,
		openExpire:   0,
		threshold:    threshold,
		cycle:        0,
	}
}

func (cb *CricuitBreaker) Execute(f func() bool) error {
	state, cycle, err := cb.beforeExecute()
	if err != nil {
		return err
	}
	success := f()
	cb.afterExecute(success, state, cycle)
	return nil
}

func (cb *CricuitBreaker) beforeExecute() (uint32, uint64, error) {
	now := time.Now().Unix()
	state, cycle := cb.refreshState(now)
	if state == StateOpen {
		return state, cycle, errors.New("cricuit breaker open")
	} else if state == StateHalfOpen && cb.s.requests > cb.threshold {
		return state, cycle, errors.New("too many requests")
	}

	return state, cycle, nil
}

func (cb *CricuitBreaker) afterExecute(success bool, state uint32, cycle uint64) {
	if cycle != cb.cycle {
		return
	}
	now := time.Now().Unix()
	if success {
		cb.onSuccess(state, now)
	} else {
		cb.onFailure(state, now)
	}
}

func (cb *CricuitBreaker) onSuccess(state uint32, now int64) {
	switch state {
	case StateClosed:
		cb.s.success()
	case StateHalfOpen:
		if cb.s.success() >= cb.threshold {
			cb.switchState(StateHalfOpen, StateClosed, now)
		}
	}
}

func (cb *CricuitBreaker) onFailure(state uint32, now int64) {
	switch state {
	case StateClosed:
		if cb.s.fail() >= cb.threshold {
			cb.switchState(StateClosed, StateOpen, now)
		}
	case StateHalfOpen:
		cb.s.fail()
		cb.switchState(StateHalfOpen, StateOpen, now)
	}
}

func (cb *CricuitBreaker) refreshState(now int64) (uint32, uint64) {
	if cb.state == StateOpen && now > cb.openExpire {
		cycle := cb.switchState(StateOpen, StateHalfOpen, now)
		return cb.state, cycle
	}
	return cb.state, cb.cycle
}

func (cb *CricuitBreaker) switchState(currentState, newState uint32, now int64) uint64 {
	if atomic.CompareAndSwapUint32(&cb.state, currentState, newState) {
		return cb.newCycle(newState, now)
	} else {
		return cb.cycle
	}
}

func (cb *CricuitBreaker) newCycle(state uint32, now int64) uint64 {
	var nextExpire int64
	switch state {
	case StateOpen:
		nextExpire = now + cb.openInterval
	case StateHalfOpen, StateClosed:
		nextExpire = 0
	}
	if atomic.CompareAndSwapInt64(&cb.openExpire, cb.openExpire, nextExpire) {
		return atomic.AddUint64(&cb.cycle, 1)
	} else {
		return cb.cycle
	}
}
