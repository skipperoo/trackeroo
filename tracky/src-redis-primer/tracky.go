package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime/debug"
	"syscall"
	"tracky/trackeroo"
)

func main() {
	fmt.Println(trackeroo.ART)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)

	round := 1
	for {
		done := make(chan struct{})

		go func(r int) {
			runLoop(r)
			close(done)
		}(round)

		select {
		case <-sigs:
			Terminate()
			fmt.Println("Terminated task manager, waiting...")
			trackeroo.Millisleep(2000)
			os.Exit(0)
		case <-done:
			round++
		}
	}
}

func runLoop(round int) {
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered from panic in round %d: %v\n", round, r)
			fmt.Printf("Stack trace:\n%s\n", string(debug.Stack()))

			Terminate()
			fmt.Println("Terminated task manager, waiting...")
			trackeroo.Millisleep(2000)
		}
	}()
	Init()
	Loop()
}
