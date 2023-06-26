package main

import (
	"errors"
	"sync/atomic"
	"time"
)

const (
	StateClosed   uint32 = 1 // 关闭状态，所有请求均会执行
	StateHalfOpen uint32 = 2 // 半开启状态，只有部分请求会被执行
	StateOpen     uint32 = 3 // 开启状态，所有请求均不会执行
)

var (
	ErrTooManyRequests = errors.New("too many requests")
	ErrOpenState       = errors.New("circuit breaker is open")
)

// statistic ...
type statistic struct {
	requests            uint32 // 熔断器通过的请求数
	continuousSuccesses uint32 // 连续成功的请求数
	continuousFailures  uint32 // 连续失败的请求数
}

func (s *statistic) request() {
	atomic.AddUint32(&s.requests, 1)
}

func (s *statistic) success() uint32 {
	atomic.StoreUint32(&s.continuousFailures, 0)
	return atomic.AddUint32(&s.continuousSuccesses, 1)
}

func (s *statistic) failure() uint32 {
	atomic.StoreUint32(&s.continuousSuccesses, 0)
	return atomic.AddUint32(&s.continuousFailures, 1)
}

func (s *statistic) clear() {
	atomic.StoreUint32(&s.requests, 0)
	atomic.StoreUint32(&s.continuousSuccesses, 0)
	atomic.StoreUint32(&s.continuousFailures, 0)
}

type CricuitBreaker struct {
	// state 熔断器状态
	// 默认为关闭状态，连续失败超过阈值后切换到开启状态
	// 关闭->开启：连续失败超过阈值
	// 开启->半开启：经过openInterval的时间后切换
	// 半开启->开启：有一次请求失败
	// 半开启->关闭：时间周期内连续成功超过阈值
	state uint32
	// openInterval 熔断器开启的时间周期
	openInterval int64
	// openExpire 熔断器开启状态的失效时间，过了这个时间后状态转变为半开启状态
	openExpire int64
	// 时间周期内连续失败超过此值熔断器开启
	// 时间周期内连续成功超过此值熔断器从半开启切换到关闭状态，并且半开启状态下最多接收这么多请求
	threshold uint32
	s         *statistic

	cycle uint32
}

func NewCricuitBreaker(openInterval int64, threshold uint32) *CricuitBreaker {
	if openInterval <= 0 {
		openInterval = 60
	}
	if threshold <= 0 {
		threshold = 5
	}
	return &CricuitBreaker{
		state:        StateClosed,
		openInterval: openInterval,
		threshold:    threshold,
		openExpire:   0,
		s: &statistic{
			requests:            0,
			continuousSuccesses: 0,
			continuousFailures:  0,
		},
		cycle: 0,
	}
}

func (cb *CricuitBreaker) Execute(f func() bool) error {
	cycle, err := cb.beforeExecute()
	if err != nil {
		return err
	}
	cb.afterExecute(cycle, f())
	return nil
}

func (cb *CricuitBreaker) beforeExecute() (uint32, error) {
	now := time.Now().Unix()
	state, cycle := cb.refreshState(now)
	if state == StateOpen {
		return cycle, ErrOpenState
	} else if state == StateHalfOpen && cb.s.requests >= cb.threshold {
		return cycle, ErrTooManyRequests
	}
	cb.s.request()
	return cycle, nil
}

func (cb *CricuitBreaker) afterExecute(cycle uint32, success bool) {
	now := time.Now().Unix()
	state, newCycle := cb.refreshState(now)
	if cycle != newCycle { // 其它请求导致熔断器状态发生变化，不做后续操作
		return
	}
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
		if cb.s.failure() >= cb.threshold {
			cb.switchState(StateClosed, StateOpen, now)
		}
	case StateHalfOpen:
		cb.s.failure()
		cb.switchState(StateHalfOpen, StateOpen, now)
	case StateOpen:
		cb.s.failure()
	}
}

func (cb *CricuitBreaker) refreshState(now int64) (state, cycle uint32) {
	expire := cb.openExpire
	if cb.state == StateOpen && expire < now {
		// 熔断器处于开启状态，并且已经经过了一个时间周期，状态切换为半开启状态
		cb.switchState(StateOpen, StateHalfOpen, now)
	}

	return cb.state, cb.cycle
}

func (cb *CricuitBreaker) switchState(oldState, newState uint32, now int64) {
	if atomic.CompareAndSwapUint32(&cb.state, oldState, newState) {
		cb.newCycle(newState, now)
	}
}

func (b *CricuitBreaker) newCycle(state uint32, now int64) {
	if atomic.CompareAndSwapUint32(&b.cycle, b.cycle, b.cycle+1) {
		b.s.clear()
		expire := b.openExpire
		var newExpire int64
		switch state {
		case StateOpen:
			newExpire = now + b.openInterval
		case StateHalfOpen, StateClosed:
			newExpire = 0
		}
		atomic.CompareAndSwapInt64(&b.openExpire, expire, newExpire)
	}
}
