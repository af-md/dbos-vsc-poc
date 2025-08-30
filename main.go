package main

import (
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"github.com/redis/go-redis/v9"
)

type Impression struct {
	MachineID string    `json:"machine_id"`
	CreatedAt time.Time `json:"created_at"`
}

type ImpressionsEvent struct {
	Impressions []Impression `json:"impressions"`
}

var (
	dbosContext dbos.DBOSContext
	redisClient *redis.Client
)

func init() {
	gob.Register(Snapshot{})
	gob.Register(DeleteKeyResult{})
}

func impressionIngestionHanlder(w http.ResponseWriter, r *http.Request) {
	var input ImpressionsEvent
	err := json.NewDecoder(r.Body).Decode(&input)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	wf, err := dbos.RunAsWorkflow(dbosContext, IngestImpressionWorkflow, input)
	if err != nil {
		panic(err)
	}

	result, err := wf.GetResult()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Result: %s\n", result)

	json.NewEncoder(w).Encode(map[string]string{"result": result})
}

func main() {

	password := url.QueryEscape(os.Getenv("PGPASSWORD"))
	if password == "" {
		panic(fmt.Errorf("PGPASSWORD environment variable not set"))
	}
	databaseURL := fmt.Sprintf("postgres://postgres:%s@localhost:5432/dbos?sslmode=disable", password)
	os.Setenv("DBOS_DATABASE_URL", databaseURL)

	var err error
	dbosContext, err = dbos.NewDBOSContext(dbos.Config{
		AppName:     "loan-app",
		DatabaseURL: databaseURL,
	})
	if err != nil {
		panic(err)
	}

	dbos.RegisterWorkflow(dbosContext, IngestImpressionWorkflow)

	dbos.RegisterWorkflow(dbosContext, PrintScheduledWorkflow, dbos.WithSchedule("*/5 * * * * *")) // every minute

	err = dbosContext.Launch()
	if err != nil {
		panic(err)
	}

	// redis

	// establish connection with Redis
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // No password set
		DB:       0,  // Use default DB
		Protocol: 2,  // Connection protocol
	})

	// test redis connection
	ctx := context.Background()

	status := redisClient.Ping(ctx)
	if status.Err() != nil {
		panic(status.Err())
	}
	fmt.Println("Connected to Redis, status:", status.String())

	http.HandleFunc("/ingestImpressions", impressionIngestionHanlder)

	fmt.Println("Server starting on :8080")
	http.ListenAndServe(":8080", nil)

	defer dbosContext.Cancel()
}
