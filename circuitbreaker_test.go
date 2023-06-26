package main

import (
	"runtime"
	"sync"
	"testing"
	"time"
)

func success(cb *CircuitBreaker) error {
	return cb.Execute(func() bool { return true })
}

func fail(cb *CircuitBreaker) error {
	return cb.Execute(func() bool { return false })
}

func TestCircuitBreaker(t *testing.T) {
	cb := NewCircuitBreaker(1, 10)
	count := 10
	wg := &sync.WaitGroup{}
	wg.Add(count - 1)
	// closed
	for i := 0; i < count-1; i++ {
		go func() {
			_ = fail(cb)
			wg.Done()
		}()
	}
	wg.Wait()
	for i := 0; i < 5; i++ {
		err := success(cb)
		if err != nil {
			t.Fatal(err)
		}
	}

	// open
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			_ = fail(cb)
			wg.Done()
		}()
	}
	wg.Wait()
	for i := 0; i < 5; i++ {
		err := success(cb)
		if err != ErrOpenState {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Second * 2)
	// half open
	_ = fail(cb) // open
	for i := 0; i < 20; i++ {
		err := success(cb)
		if err != ErrOpenState {
			t.Fatal(err)
		}
	}
	// half open
	time.Sleep(time.Second * 2)
	for i := 0; i < 20; i++ { // close
		err := success(cb)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestCircuitBreakerParallel(t *testing.T) {
	const num = 10000
	cpu := runtime.NumCPU()
	total := num * cpu
	result := make(chan error, total)
	cb := NewCircuitBreaker(1, 10)
	routine := func() {
		for i := 0; i < num; i++ {
			result <- success(cb)
		}
	}
	for i := 0; i < cpu; i++ {
		go routine()
	}
	for i := 0; i < total; i++ {
		err := <-result
		if err != nil {
			t.Fatal(err)
		}
	}
}
