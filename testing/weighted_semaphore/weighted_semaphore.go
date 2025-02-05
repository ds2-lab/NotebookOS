package main

import (
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
	"math/rand/v2"
	"time"
)

func main() {
	numReplicas := 3
	sem := semaphore.NewWeighted(int64(numReplicas))

	err := sem.Acquire(context.Background(), 1)
	fmt.Println("Called Acquire.")
	if err != nil {
		panic(err)
	}

	err = sem.Acquire(context.Background(), 1)
	fmt.Println("Called Acquire.")
	if err != nil {
		panic(err)
	}

	err = sem.Acquire(context.Background(), 1)
	fmt.Println("Called Acquire.")
	if err != nil {
		panic(err)
	}

	for i := 0; i < numReplicas; i++ {
		go func() {
			time.Sleep(time.Millisecond * time.Duration(10+rand.Int64N(25)))

			sem.Release(1)
			fmt.Println("Called release.")
		}()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*1)
	defer cancel()

	notifyChan := make(chan struct{})

	go func() {
		err := sem.Acquire(ctx, int64(numReplicas))
		fmt.Printf("Error: %v\n", err)

		if err == nil {
			notifyChan <- struct{}{}
		} else {
			panic(err)
		}
	}()

	select {
	case <-ctx.Done():
		{
			fmt.Println("Context expired.")
		}
	case <-notifyChan:
		{
			fmt.Println("Received notification.")
		}
	}
}
