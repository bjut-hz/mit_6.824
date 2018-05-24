package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//



func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	idleWorkers := make(chan string, ntasks * n_other)
	done := make(chan bool)
	go func() {
		for {
			select {
			case workAddress := <-registerChan:
				idleWorkers <- workAddress
			case <- done:
				break
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		wg.Add(1)
		go func(taskNum int) {
			debug("DEBUG: current taskNum: %v, NumOtherPhase: %v, jobPhase: %v \n", taskNum, n_other, phase)
			tryTimes := 0
			for tryTimes < 10 {
				workerAddress := <- idleWorkers
				taskArgs := DoTaskArgs{JobName:jobName, File:mapFiles[taskNum], Phase:phase, TaskNumber:taskNum, NumOtherPhase:n_other}
				ret := call(workerAddress, "Worker.DoTask", &taskArgs, new(struct{}))

				if ret {
					// return idle worker back
					registerChan <- workerAddress
					wg.Done()
					break
				} else {
					tryTimes++
					// do task error. worker may in error state.
					debug("DEBUG: DoTask error. address: %v, tryTimes: %v", workerAddress, tryTimes)
				}
			}
		}(i)
	}
	wg.Wait()
	done <- true

	fmt.Printf("Schedule: %v done\n", phase)
}
