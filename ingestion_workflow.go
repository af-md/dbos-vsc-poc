package main

import (
	"context"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
)


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
