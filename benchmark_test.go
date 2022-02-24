package benchmark_test

import (
	"benchmark"
	"os"
	"testing"
	"time"
)

func subtask1() error {
	time.Sleep(1 * time.Millisecond)
	return nil
}

func subtask2() error {
	time.Sleep(2 * time.Millisecond)
	return nil
}

func TestBenchmark(t *testing.T) {
	var (
		b = benchmark.Benchmark{
			WorkerCount:  10,
			Duration:     10 * time.Second,
			LatencyStart: 1 * time.Millisecond,
			LatencyStep:  6,
		}
		r1 = benchmark.NewResult("Sub-task 1")
		r2 = benchmark.NewResult("Sub-task 2")
	)
	r1.SetLatencyInterval(1*time.Millisecond, 6)
	r2.SetLatencyInterval(1*time.Millisecond, 6)
	r := b.Run("Task", func(i int) error {
		r1.RunTask(subtask1)
		r2.RunTask(subtask2)
		return nil
	})
	r.Report(os.Stdout)
	r1.Report(os.Stdout)
	r2.Report(os.Stdout)
}
