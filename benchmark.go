package benchmark

import (
	"fmt"
	"io"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"
	"unsafe"
)

type LatencyInterval struct {
	LowerBound time.Duration
	UpperBound time.Duration
	Count      int64
}

type Result struct {
	Name             string
	PassedCount      int64
	FailedCount      int64
	MinLatency       time.Duration
	MaxLatency       time.Duration
	TotalLatency     time.Duration
	StartTime        *time.Time
	FinishTime       *time.Time
	LatencyIntervals []*LatencyInterval
	errMtx           sync.Mutex
	Errors           []error
	concurrency      int64
}

func NewResult(name string) *Result {
	return &Result{
		Name:       name,
		MinLatency: math.MaxInt64,
	}
}

func (r *Result) wrapTask(f func(int) error) func(int) error {
	return func(i int) error {
		atomic.AddInt64(&r.concurrency, 1)
		start := time.Now()
		err := f(i)
		latency := time.Since(start)
		atomic.AddInt64(&r.concurrency, -1)
		if err != nil {
			atomic.AddInt64(&r.FailedCount, 1)
		} else {
			r.Pass(latency)
		}
		return err
	}
}

func (r *Result) SetLatencyInterval(startInterval time.Duration, step int) {
	var (
		lowerBound time.Duration
		upperBound = startInterval
	)
	r.LatencyIntervals = make([]*LatencyInterval, step+2)
	for i := 0; i <= step; i++ {
		r.LatencyIntervals[i] = &LatencyInterval{
			LowerBound: lowerBound,
			UpperBound: upperBound,
		}
		lowerBound = upperBound
		upperBound *= 2
	}
	r.LatencyIntervals[step+1] = &LatencyInterval{
		LowerBound: lowerBound,
		UpperBound: time.Duration(math.MaxInt64),
	}
}

func (r *Result) RunTask(f func() error) {
	atomic.AddInt64(&r.concurrency, 1)
	start := time.Now()
	err := f()
	end := time.Now()
	atomic.AddInt64(&r.concurrency, -1)
	atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(&r.StartTime)), unsafe.Pointer(nil), unsafe.Pointer(&start))
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&r.FinishTime)), unsafe.Pointer(&end))
	if err != nil {
		r.Fail(err)
	} else {
		r.Pass(end.Sub(start))
	}
}

func (r *Result) Pass(latency time.Duration) {
	atomic.AddInt64(&r.PassedCount, 1)
	atomic.AddInt64((*int64)(&r.TotalLatency), int64(latency))
	for _, interval := range r.LatencyIntervals {
		if latency <= interval.UpperBound && latency > interval.LowerBound {
			atomic.AddInt64(&interval.Count, 1)
			break
		}
	}
	for {
		maxLatency := time.Duration(atomic.LoadInt64((*int64)(&r.MaxLatency)))
		if maxLatency >= latency || atomic.CompareAndSwapInt64((*int64)(&r.MaxLatency), int64(maxLatency), int64(latency)) {
			break
		}
	}
	for {
		minLatency := time.Duration(atomic.LoadInt64((*int64)(&r.MinLatency)))
		if minLatency <= latency || atomic.CompareAndSwapInt64((*int64)(&r.MinLatency), int64(minLatency), int64(latency)) {
			break
		}
	}
}

func (r *Result) Fail(err error) {
	atomic.AddInt64(&r.FailedCount, 1)
	r.errMtx.Lock()
	r.Errors = append(r.Errors, err)
	r.errMtx.Unlock()
}

func (r *Result) GetDuration() time.Duration {
	return r.FinishTime.Sub(*r.StartTime)
}

func (r *Result) GetPassRate() float64 {
	return float64(r.PassedCount) / float64(r.PassedCount+r.FailedCount)
}

func (r *Result) GetTPS() int64 {
	return int64(float64(r.PassedCount+r.FailedCount) * float64(time.Second) / float64(r.GetDuration()))
}

func (r *Result) GetAvgLatency() time.Duration {
	return time.Duration(int64(r.TotalLatency) / r.PassedCount)
}

func (r *Result) GetConcurrency() int64 {
	return atomic.LoadInt64(&r.concurrency)
}

func (r *Result) Report(w io.Writer) *Result {
	if _, err := fmt.Fprintf(w, "%s (%s)\n", r.Name, r.GetDuration()); err != nil {
		panic(err)
	}
	tw := tabwriter.NewWriter(w, 0, 0, 0, ' ', 0)
	fmt.Fprintf(tw, " * Passed/Failed\t: %d/%d (%.2f%%)\n", r.PassedCount, r.FailedCount, r.GetPassRate()*100)
	fmt.Fprintf(tw, " * Avg TPS\t: %d\n", r.GetTPS())
	if r.PassedCount > 0 {
		fmt.Fprintf(tw, " * Avg Latency\t: %s\n", r.GetAvgLatency())
		fmt.Fprintf(tw, " * Min Latency\t: %s\n", r.MinLatency)
		fmt.Fprintf(tw, " * Max Latency\t: %s\n", r.MaxLatency)
		for _, interval := range r.LatencyIntervals {
			percentage := float64(interval.Count*100) / float64(r.PassedCount)
			if interval.LowerBound == 0 {
				fmt.Fprintf(tw, " * Latency <= %s\t: %d \t(%.2f%%)\n", interval.UpperBound, interval.Count, percentage)
			} else if interval.UpperBound == math.MaxInt64 {
				fmt.Fprintf(tw, " * %s < Latency\t: %d \t(%.2f%%)\n", interval.LowerBound, interval.Count, percentage)
			} else {
				fmt.Fprintf(tw, " * %s < Latency <= %s\t: %d \t(%.2f%%)\n", interval.LowerBound, interval.UpperBound, interval.Count, percentage)
			}
		}
	}
	if err := tw.Flush(); err != nil {
		panic(err)
	}
	return r
}

func (r *Result) PrintErrors(w io.Writer, limit int) *Result {
	if limit < 1 || limit > len(r.Errors) {
		limit = len(r.Errors)
	}
	for i := 0; i < limit; i++ {
		if _, err := fmt.Fprintln(w, r.Errors[i]); err != nil {
			panic(err)
		}
	}
	return r
}

type Benchmark struct {
	WorkerCount  int
	TaskCount    int
	Duration     time.Duration
	Tps          int
	LatencyStart time.Duration
	LatencyStep  int
	ShowProcess  bool
}

func (b *Benchmark) Run(name string, f func(i int) error) *Result {
	var (
		result      = NewResult(name)
		workerCount = b.WorkerCount
		duration    = b.Duration
		ok          chan bool
		done        chan bool
	)
	if b.LatencyStart > 0 {
		result.SetLatencyInterval(b.LatencyStart, b.LatencyStep)
	}
	if b.WorkerCount <= 0 {
		workerCount = runtime.NumCPU()
	}
	if b.Duration <= 0 {
		duration = 30 * time.Second
	}
	if b.ShowProcess {
		ok = make(chan bool)
		done = make(chan bool)
		go func() {
			var (
				ticker           = time.NewTicker(1 * time.Second)
				lastReqCount     int64
				lastTotalLatency time.Duration
			)
		loop:
			for {
				select {
				case <-ticker.C:
					fmt.Printf("\033[2K\n\033[2K\n\033[2K\n\033[2K\n\033[2K\n\033[2K\033[5A")
					startTime := (*time.Time)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&result.StartTime))))
					if startTime == nil {
						continue
					}
					runDuration := time.Since(*startTime)
					passedCount := atomic.LoadInt64(&result.PassedCount)
					failedCount := atomic.LoadInt64(&result.FailedCount)
					concurrency := atomic.LoadInt64(&result.concurrency)
					currentTotalLatency := time.Duration(atomic.LoadInt64((*int64)(&result.TotalLatency)))
					currentReqCount := passedCount + failedCount
					tps := currentReqCount - lastReqCount
					passRate := float64(passedCount*100) / float64(currentReqCount)
					var latency time.Duration
					if tps > 0 {
						latency = time.Duration(int64(currentTotalLatency-lastTotalLatency) / tps)
					}
					var process int
					if b.TaskCount > 0 {
						process = int((float64(currentReqCount) / float64(b.TaskCount)) * 25)
					} else {
						process = int(float64(runDuration) / float64(duration) * 25)
					}
					fmt.Printf("%s (%s)\n", name, runDuration)
					fmt.Printf(" * Passed/Failed: %d/%d (%.2f%%)\n", passedCount, failedCount, passRate)
					fmt.Printf(" * TPS          : %d\n", tps)
					fmt.Printf(" * Latency      : %s\n", latency)
					fmt.Printf(" * Concurrency  : %d\n", concurrency)
					fmt.Printf(" * Process      : [%s%s]\033[5A", strings.Repeat("#", process), strings.Repeat(" ", 25-process))
					lastReqCount = currentReqCount
					lastTotalLatency = currentTotalLatency
				case <-done:
					ticker.Stop()
					break loop
				}
			}
			fmt.Printf("\033[2K\n\033[2K\n\033[2K\n\033[2K\n\033[2K\n\033[2K\033[5A")
			ok <- true
		}()
	}
	f = result.wrapTask(f)
	switch {
	case b.TaskCount > 0 && b.Tps <= 0:
		benchmark1(workerCount, b.TaskCount, f, result)
	case b.TaskCount > 0 && b.Tps > 0:
		benchmark2(workerCount, b.TaskCount, b.Tps, f, result)
	case b.TaskCount <= 0 && b.Tps <= 0:
		benchmark3(workerCount, duration, f, result)
	case b.TaskCount <= 0 && b.Tps > 0:
		benchmark4(workerCount, duration, b.Tps, f, result)
	}
	if b.ShowProcess {
		done <- true
		<-ok
	}
	return result
}

func benchmark1(workerCount int, taskCount int, f func(int) error, result *Result) {
	var (
		wg        sync.WaitGroup
		cur       = int64(-1)
		errorList = make([][]error, workerCount)
	)
	wg.Add(workerCount)
	startTime := time.Now()
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&result.StartTime)), unsafe.Pointer(&startTime))
	for i := 0; i < workerCount; i++ {
		j := i
		go func() {
			var errors []error
			for {
				k := int(atomic.AddInt64(&cur, 1))
				if k >= taskCount {
					break
				}
				if err := f(k); err != nil {
					errors = append(errors, err)
				}
			}
			errorList[j] = errors
			wg.Done()
		}()
	}
	wg.Wait()
	finishTime := time.Now()
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&result.FinishTime)), unsafe.Pointer(&finishTime))
	for _, errors := range errorList {
		result.Errors = append(result.Errors, errors...)
	}
}

func benchmark2(workerCount int, taskCount int, tps int, f func(int) error, result *Result) {
	var (
		wg        sync.WaitGroup
		taskQueue = make(chan int, 1000000)
		errorList = make([][]error, workerCount)
		tp5ms     = tps / 200
	)
	wg.Add(workerCount)
	startTime := time.Now()
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&result.StartTime)), unsafe.Pointer(&startTime))
	for i := 0; i < workerCount; i++ {
		j := i
		go func() {
			var errors []error
			for k := range taskQueue {
				if err := f(k); err != nil {
					errors = append(errors, err)
				}
			}
			errorList[j] = errors
			wg.Done()
		}()
	}
	next := time.Now()
	if tps%200 == 0 {
	loop1:
		for k := 0; k < taskCount; {
			for i := 0; i < tp5ms; i++ {
				taskQueue <- k
				k++
				if k >= taskCount {
					break loop1
				}
			}
			next = next.Add(5 * time.Millisecond)
			time.Sleep(next.Sub(time.Now()))
		}
	} else {
		a := tps % 200
		b := 200 / a
		c := 0
		d := a * b
	loop2:
		for k := 0; k < taskCount; {
			for i := 0; i < tp5ms; i++ {
				taskQueue <- k
				k++
				if k >= taskCount {
					break loop2
				}
			}
			if c%b == 0 && c%200 < d {
				taskQueue <- k
				k++
				if k >= taskCount {
					break loop2
				}
			}
			next = next.Add(5 * time.Millisecond)
			time.Sleep(next.Sub(time.Now()))
			c++
		}
	}
	close(taskQueue)
	wg.Wait()
	finishTime := time.Now()
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&result.FinishTime)), unsafe.Pointer(&finishTime))
	for _, errors := range errorList {
		result.Errors = append(result.Errors, errors...)
	}
}

func benchmark3(workerCount int, duration time.Duration, f func(int) error, result *Result) {
	var (
		wg        sync.WaitGroup
		cur       = int64(-1)
		errorList = make([][]error, workerCount)
	)
	wg.Add(workerCount)
	startTime := time.Now()
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&result.StartTime)), unsafe.Pointer(&startTime))
	for i := 0; i < workerCount; i++ {
		j := i
		go func() {
			var (
				errors []error
				timer  = time.NewTimer(duration)
			)
		loop:
			for {
				select {
				case <-timer.C:
					break loop
				default:
					k := int(atomic.AddInt64(&cur, 1))
					if err := f(k); err != nil {
						errors = append(errors, err)
					}
				}
			}
			errorList[j] = errors
			wg.Done()
		}()
	}
	wg.Wait()
	finishTime := time.Now()
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&result.FinishTime)), unsafe.Pointer(&finishTime))
	for _, errors := range errorList {
		result.Errors = append(result.Errors, errors...)
	}
}

func benchmark4(workerCount int, duration time.Duration, tps int, f func(int) error, result *Result) {
	var (
		wg        sync.WaitGroup
		taskQueue = make(chan int, 1000000)
		errorList = make([][]error, workerCount)
		tp5ms     = tps / 200
	)
	wg.Add(workerCount)
	startTime := time.Now()
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&result.StartTime)), unsafe.Pointer(&startTime))
	for i := 0; i < workerCount; i++ {
		j := i
		go func() {
			var (
				errors []error
				timer  = time.NewTimer(duration)
			)
		loop:
			for {
				select {
				case k := <-taskQueue:
					if err := f(k); err != nil {
						errors = append(errors, err)
					}
				case <-timer.C:
					for range taskQueue {
						//Consume remaining tasks.
					}
					break loop
				}
			}
			errorList[j] = errors
			wg.Done()
		}()
	}
	var (
		k     = 0
		next  = time.Now()
		timer = time.NewTimer(duration)
	)
	if tps%200 == 0 {
	loop1:
		for {
			select {
			case <-timer.C:
				break loop1
			default:
				for i := 0; i < tp5ms; i++ {
					taskQueue <- k
					k++
				}
				next = next.Add(5 * time.Millisecond)
				time.Sleep(next.Sub(time.Now()))
			}
		}
	} else {
		a := tps % 200
		b := 200 / a
		c := 0
		d := a * b
	loop2:
		for {
			select {
			case <-timer.C:
				break loop2
			default:
				for i := 0; i < tp5ms; i++ {
					taskQueue <- k
					k++
				}
				if c%b == 0 && c%200 < d {
					taskQueue <- k
					k++
				}
				next = next.Add(5 * time.Millisecond)
				time.Sleep(next.Sub(time.Now()))
				c++
			}
		}
	}
	close(taskQueue)
	wg.Wait()
	finishTime := time.Now()
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&result.FinishTime)), unsafe.Pointer(&finishTime))
	for _, errors := range errorList {
		result.Errors = append(result.Errors, errors...)
	}
}
