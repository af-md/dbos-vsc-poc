package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

type Snapshot struct {
	MachineID       string
	ImpressionStart string
	ImpressionEnd   string
}

type DeleteKeyResult struct {
	Deleted  bool
	NotFound bool
}

// Questions:
// 1. What happpens when there are multiple workflows running on the redis instance and they all try to sessionise. Could conflict arise?


func PrintScheduledWorkflow(dbosCtx dbos.DBOSContext, scheduledTime time.Time) (Snapshot, error) {
	fmt.Println("Scheduled time: ", scheduledTime)

	snapshot, err := dbos.RunAsStep(dbosCtx, func(ctx context.Context) (Snapshot, error) {
		return findKeyForSession(ctx)
	})

	if err != nil {
		// print the error
		fmt.Println("Error finding key for session: ", err)
		return Snapshot{}, err
	}

	fmt.Println("Found key for sessionising: ", snapshot.MachineID)
	fmt.Println("With impression start: ", snapshot.ImpressionStart)
	fmt.Println("With impression end: ", snapshot.ImpressionEnd)

	deleteKeyResult, err := dbos.RunAsStep(dbosCtx, func(ctx context.Context) (DeleteKeyResult, error) {
		return deleteKeyFromRedis(ctx, snapshot.MachineID)
	})

	if err != nil {
		fmt.Println("Error deleting key from redis: ", err)
		return snapshot, err
	}

	if deleteKeyResult.NotFound {
		// gracefully exit the workflow
		fmt.Println("Key not found")
		fmt.Println("Different workflow deleted the key")
		return snapshot, nil
	}

	_, err = dbos.RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return sendSnapshotToExternalService(ctx, snapshot)
	})

	if err != nil {
		fmt.Println("Error sending snapshot to external service: ", err)
		return snapshot, err
	}

	return snapshot, nil
}

func findKeyForSession(ctx context.Context) (Snapshot, error) {
	// Get first key
	iter := redisClient.Scan(ctx, 0, "machine*", 1).Iterator()

	if !iter.Next(ctx) {
		return Snapshot{}, fmt.Errorf("no keys found")
	}

	key := iter.Val()

	values := redisClient.LRange(ctx, key, 0, -1).Val()

	if len(values) == 0 {
		return Snapshot{}, fmt.Errorf("key %s has no values", key)
	}

	snapshot := Snapshot{
		MachineID:       key,
		ImpressionStart: values[0],
		ImpressionEnd:   values[len(values)-1],
	}

	return snapshot, nil
}

func deleteKeyFromRedis(ctx context.Context, input string) (DeleteKeyResult, error) {
	res, err := redisClient.Del(ctx, input).Result()
	if err != nil {
		return DeleteKeyResult{}, err
	}

	if res == 0 {
		fmt.Println("key wasn't found to delete: ", input)
		fmt.Print("gracefully exiting the workflow")
		return DeleteKeyResult{NotFound: true}, nil
	}

	fmt.Println("Deleted key from redis: ", input)
	return DeleteKeyResult{Deleted: true}, nil
}

func sendSnapshotToExternalService(ctx context.Context, input Snapshot) (string, error) {
	// Simulate writing to external service
	time.Sleep(1 * time.Second)
	fmt.Println("Wrote snapshot to external service: ", input)
	return "Wrote snapshot to external service", nil
}
