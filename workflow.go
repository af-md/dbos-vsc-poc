package main

import (
	"context"
	"fmt"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)

type Snapshot struct {
	MachineID       string
	ImpressionStart string // this should be time
	ImpressionEnd   string // this should be time TODO
}

// Simple scheduled workflow that just prints
func PrintScheduledWorkflow(dbosCtx dbos.DBOSContext, scheduledTime time.Time) (string, error) {

	// step 1 - find the key that needs sessionising (return snapshot with key)
	// step 2 - delete the key from redis
	// if key data was not found, then gracefully exit the workflow because the key was deleted
	// step 3 - write the snapshot to the external service

	fmt.Println("Scheduled time: ", scheduledTime)

	result, err := dbos.RunAsStep(dbosCtx, func(ctx context.Context) (Snapshot, error) {
		return findKeyForSession(ctx)
	})

	if err != nil {
		// print the error
		fmt.Println("Error finding key for session: ", err)
		return "", err
	}

	// print time
	fmt.Println("Scheduled time: ", scheduledTime)

	fmt.Println("Found key for sessionising: ", result.MachineID)
	fmt.Println("With impression start: ", result.ImpressionStart)
	fmt.Println("With impression end: ", result.ImpressionEnd)

	return result.MachineID, nil
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

// func deleteKeyFromRedis(ctx context.Context, input Snapshot) (string, error) {

// 	return "Ingested to Redis", nil
// }

// redis ingestion flow

func IngestImpressionWorkflow(dbosCtx dbos.DBOSContext, input ImpressionsEvent) (string, error) {
	result2, err := dbos.RunAsStep(dbosCtx, func(ctx context.Context) (string, error) {
		return ingestToRedisStep(ctx, input)
	})
	if err != nil {
		return "", err
	}
	return result2, nil
}

func ingestToRedisStep(ctx context.Context, input ImpressionsEvent) (string, error) {
	for _, v := range input.Impressions {
		err := redisClient.RPush(ctx, v.MachineID, v.CreatedAt).Err()
		if err != nil {
			return "", err
		}
	}

	return "Ingested to Redis", nil
}
