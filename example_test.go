package taskqueue_test

import (
	"context"
	"fmt"

	taskqueue "github.com/skme902/tq"
	"github.com/skme902/tq/store/memstore"
)

func Example() {
	eng, err := taskqueue.New(taskqueue.Options{Store: memstore.New()})
	if err != nil {
		panic(err)
	}
	defer eng.Stop()

	ctx := context.Background()
	key := taskqueue.QueueKey{Namespace: "acme", Name: "emails"}

	// Queues are created on first use. Lower PriorityKey = more urgent (1..5, 0 = default 3).
	for i, pri := range []int32{5, 1, 0} {
		_, _ = eng.AddTask(ctx, key, taskqueue.Task{
			Payload:  []byte(fmt.Sprintf("job-%d", i)),
			Priority: taskqueue.Priority{PriorityKey: pri},
		})
	}

	for range 3 {
		d, err := eng.PollTask(ctx, key, taskqueue.PollOptions{Identity: "worker-1"})
		if err != nil {
			panic(err)
		}
		fmt.Println(string(d.Task.Payload))
		_ = d.Ack() // or d.Nack() / let the lease expire to redeliver
	}
	// Unordered output:
	// job-0
	// job-1
	// job-2
}
