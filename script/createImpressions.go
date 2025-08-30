package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type Impression struct {
	MachineID string    `json:"machine_id"`
	CreatedAt time.Time `json:"created_at"`
}

type ImpressionsEvent struct {
	Impressions []Impression `json:"impressions"`
}

func main() {
	// Create a batch of impressions
	var impressions []Impression

	// Generate some sample machine IDs and create impressions
	machineIDs := []string{
		"machine-001",
		"machine-002",
		"machine-003",
		"machine-004",
		"machine-005",
	}

	// Create multiple impressions per machine
	for _, machineID := range machineIDs {
		// Create 3-5 impressions per machine with slight time variations
		numImpressions := 3 + (len(machineID) % 3) // Varies between 3-5

		for i := 0; i < numImpressions; i++ {
			impression := Impression{
				MachineID: machineID,
				CreatedAt: time.Now().Add(time.Duration(-i) * time.Second), // Slight time offsets
			}
			impressions = append(impressions, impression)
		}
	}

	// Create the event payload
	event := ImpressionsEvent{
		Impressions: impressions,
	}

	// Convert to JSON
	jsonData, err := json.Marshal(event)
	if err != nil {
		log.Fatal("Error marshaling JSON:", err)
	}

	fmt.Printf("Sending %d impressions to server...\n", len(impressions))

	// Make POST request
	resp, err := http.Post("http://localhost:8080/ingestImpressions", "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Fatal("Error making request:", err)
	}
	defer resp.Body.Close()

	// Read response
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Error reading response:", err)
	}

	fmt.Printf("Response Status: %s\n", resp.Status)
	fmt.Printf("Response Body: %s\n", string(body))

	if resp.StatusCode == http.StatusOK {
		fmt.Println("✅ Impressions sent successfully!")
	} else {
		fmt.Printf("❌ Request failed with status: %s\n", resp.Status)
	}
}
