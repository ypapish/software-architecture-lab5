package integration

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

const (
	baseAddress = "http://balancer:8090"
	numRequests = 10
)

var client = http.Client{
	Timeout: 3 * time.Second,
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	const maxAttempts = 3
	var serverHits map[string]int

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		t.Logf("Attempt %d/%d", attempt, maxAttempts)
		serverHits = make(map[string]int)

		for i := 0; i < numRequests; i++ {
			resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=myteam", baseAddress))
			if err != nil {
				t.Errorf("Request failed: %v", err)
				continue
			}
			defer resp.Body.Close()

			server := resp.Header.Get("lb-from")
			if server == "" {
				t.Error("Response missing 'lb-from' header")
				continue
			}

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Errorf("Failed to read response body: %v", err)
				continue
			}
			if len(body) == 0 {
				t.Errorf("Received empty data for request %d", i+1)
			}

			t.Logf("Request %d: handled by server %s, data: %s", i+1, server, string(body))
			serverHits[server]++
		}

		if len(serverHits) >= 2 {
			break
		}

		t.Log("Retrying due to poor distribution...")
		time.Sleep(1 * time.Second)
	}

	if len(serverHits) < 2 {
		t.Errorf("Requests were not distributed to multiple servers. Got hits: %v", serverHits)
	}

	for server, hits := range serverHits {
		t.Logf("Server %s handled %d requests", server, hits)
	}
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration test is not enabled")
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(fmt.Sprintf("%s/api/v1/some-data?key=myteam", baseAddress))
		if err != nil {
			b.Error(err)
			continue
		}
		resp.Body.Close()
	}
}
