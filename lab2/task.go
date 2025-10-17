package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/hazelcast/hazelcast-go-client"
)

const (
	NumGoroutines        = 10
	IncrementsPerRoutine = 10000
	ExpectedValue        = NumGoroutines * IncrementsPerRoutine
)

func incrementTaskNoLock(ctx context.Context, client *hazelcast.Client, mapName string, wg *sync.WaitGroup) {
	defer wg.Done()

	counterMap, err := client.GetMap(ctx, mapName)
	if err != nil {
		log.Printf("Error GetMap: %v", err)
		return
	}

	for i := 0; i < IncrementsPerRoutine; i++ {
		currentValue, err := counterMap.Get(ctx, "counter")
		if err != nil {
			log.Printf("Error Get: %v", err)
			continue
		}
		val, _ := currentValue.(int32)
		_, err = counterMap.Put(ctx, "counter", val+1)
		if err != nil {
			log.Printf("Error Put: %v", err)
		}
	}
}

func incrementTaskWithPessimisticLock(ctx context.Context, client *hazelcast.Client, mapName string, wg *sync.WaitGroup) {
	defer wg.Done()

	counterMap, err := client.GetMap(ctx, mapName)
	if err != nil {
		log.Printf("Error GetMap: %v", err)
		return
	}

	lockCtx := counterMap.NewLockContext(ctx)

	for i := 0; i < IncrementsPerRoutine; i++ {
		if err := counterMap.Lock(lockCtx, "counter"); err != nil {
			log.Printf("Error Lock: %v", err)
			continue
		}

		currentValue, err := counterMap.Get(lockCtx, "counter")
		if err != nil {
			log.Printf("Error Get inside lock: %v", err)
			_ = counterMap.Unlock(lockCtx, "counter")
			continue
		}

		val, ok := currentValue.(int32)
		if !ok {
			log.Printf("Failed to cast value to int32, got: %T", currentValue)
			_ = counterMap.Unlock(lockCtx, "counter")
			continue
		}

		_, err = counterMap.Put(lockCtx, "counter", val+1)
		if err != nil {
			log.Printf("Error Put inside lock: %v", err)
			_ = counterMap.Unlock(lockCtx, "counter")
			continue
		}

		if err := counterMap.Unlock(lockCtx, "counter"); err != nil {
			log.Printf("Error Unlock: %v", err)
		}
	}
}

func incrementTaskWithOptimisticLock(ctx context.Context, client *hazelcast.Client, mapName string, wg *sync.WaitGroup) {
	defer wg.Done()

	counterMap, err := client.GetMap(ctx, mapName)
	if err != nil {
		log.Printf("Error GetMap: %v", err)
		return
	}

	for i := 0; i < IncrementsPerRoutine; i++ {
		for {
			oldValue, err := counterMap.Get(ctx, "counter")
			if err != nil {
				log.Printf("Error Get: %v", err)
				time.Sleep(10 * time.Millisecond)
				continue
			}
			val, _ := oldValue.(int32)

			newValue := val + 1

			replaced, err := counterMap.ReplaceIfSame(ctx, "counter", oldValue, newValue)
			if err != nil {
				log.Printf("Error ReplaceIfSame: %v", err)
				time.Sleep(10 * time.Millisecond)
				continue
			}

			if replaced {
				break
			}
		}
	}
}

func incrementTaskWithIAtomicLong(ctx context.Context, atomicLong *hazelcast.AtomicLong, wg *sync.WaitGroup) {
	defer wg.Done()
	for i := 0; i < IncrementsPerRoutine; i++ {
		if _, err := atomicLong.IncrementAndGet(ctx); err != nil {
			log.Printf("Error IncrementAndGet: %v", err)
		}
	}
}

func main() {
	ctx := context.Background()
	config := hazelcast.NewConfig()
	config.Cluster.Name = "lab-cluster"
	config.Cluster.Network.SetAddresses("hz-node1:5701", "hz-node2:5701", "hz-node3:5701")

	time.Sleep(5 * time.Second)

	client, err := hazelcast.StartNewClientWithConfig(ctx, config)
	if err != nil {
		log.Fatalf("Failed to start Hazelcast client: %v", err)
	}
	defer client.Shutdown(ctx)
	log.Println("Client successfully connected to the cluster.")

	runMapTest(ctx, client, "1. Test on Distributed Map without locks", "map_no_lock", incrementTaskNoLock)
	runMapTest(ctx, client, "2. Test with pessimistic locking", "map_pessimistic", incrementTaskWithPessimisticLock)
	runMapTest(ctx, client, "3. Test with optimistic locking", "map_optimistic", incrementTaskWithOptimisticLock)
	runAtomicLongTest(ctx, client)
}

func runMapTest(ctx context.Context, client *hazelcast.Client, title, mapName string, task func(context.Context, *hazelcast.Client, string, *sync.WaitGroup)) {
	fmt.Printf("\n--- %s ---\n", title)
	counterMap, err := client.GetMap(ctx, mapName)
	if err != nil {
		log.Printf("Error getting map %s: %v", mapName, err)
		return
	}
	counterMap.Put(ctx, "counter", int32(0))

	var wg sync.WaitGroup
	startTime := time.Now()
	for i := 0; i < NumGoroutines; i++ {
		wg.Add(1)
		go task(ctx, client, mapName, &wg)
	}
	wg.Wait()
	printResults(ctx, time.Since(startTime), counterMap, nil)
}

func runAtomicLongTest(ctx context.Context, client *hazelcast.Client) {
	fmt.Println("\n--- 4. Test with IAtomicLong ---")
	atomicCounter, err := client.CPSubsystem().GetAtomicLong(ctx, "atomic_long_counter")
	if err != nil {
		log.Printf("Error getting AtomicLong: %v", err)
		return
	}
	atomicCounter.Set(ctx, 0)

	var wg sync.WaitGroup
	startTime := time.Now()
	for i := 0; i < NumGoroutines; i++ {
		wg.Add(1)
		go incrementTaskWithIAtomicLong(ctx, atomicCounter, &wg)
	}
	wg.Wait()
	printResults(ctx, time.Since(startTime), nil, atomicCounter)
}

func printResults(ctx context.Context, duration time.Duration, m *hazelcast.Map, al *hazelcast.AtomicLong) {
	var finalValue interface{}
	var err error

	if m != nil {
		finalValue, err = m.Get(ctx, "counter")
	} else if al != nil {
		finalValue, err = al.Get(ctx)
	}

	if err != nil {
		log.Printf("Failed to get final value: %v", err)
		return
	}

	fmt.Printf("Expected value: %d\n", ExpectedValue)
	fmt.Printf("Actual final value: %v\n", finalValue)

	var lost int64
	switch v := finalValue.(type) {
	case int32:
		lost = int64(ExpectedValue) - int64(v)
	case int64:
		lost = int64(ExpectedValue) - v
	}

	totalOps := float64(NumGoroutines * IncrementsPerRoutine)
	throughput := totalOps / duration.Seconds()

	fmt.Printf("Lost increments: %d\n", lost)
	fmt.Printf("Execution time: %.4f seconds\n", duration.Seconds())
	fmt.Printf("Throughput: %.2f ops/sec\n", throughput)
}
