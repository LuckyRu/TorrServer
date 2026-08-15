package settings

import "sync"

// startTogether releases every caller from one point. Without it the first goroutines finish
// before the last are even created, so a test that needs two of them inside the same critical
// section at the same moment passes on broken code most of the time.
func startTogether(callers int, body func(int)) {
	var wait sync.WaitGroup
	ready := make(chan struct{}, callers)
	start := make(chan struct{})

	wait.Add(callers)
	for i := range callers {
		go func(index int) {
			defer wait.Done()
			ready <- struct{}{}
			<-start
			body(index)
		}(i)
	}
	for range callers {
		<-ready
	}
	close(start)
	wait.Wait()
}
