package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/ypapish/software-architecture-lab5/httptools"
	"github.com/ypapish/software-architecture-lab5/signal"
)

var port = flag.Int("port", 8080, "server port")

const (
	confHealthFailure = "CONF_HEALTH_FAILURE"
	dbServiceAddr     = "DB_SERVICE_ADDR"
	teamName          = "myteam"
)

func saveInitialData(dbBaseURL string) {
	currentDate := time.Now().Format("2006-01-02")
	data := map[string]string{"value": currentDate}
	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Fatal("Error marshalling initial data:", err)
	}

	maxRetries := 5
	for i := 0; i < maxRetries; i++ {
		resp, err := http.Post(dbBaseURL+"/db/"+teamName, "application/json", bytes.NewBuffer(jsonData))
		if err == nil && resp.StatusCode == http.StatusCreated {
			resp.Body.Close()
			return
		}
		if err != nil {
			log.Printf("Trr %d: Error during data saving: %v", i+1, err)
		}
		time.Sleep(2 * time.Second)
	}
	log.Fatal("Data wasn`t saved after multiply tries")
}

func main() {
	flag.Parse()

	dbBaseURL := os.Getenv(dbServiceAddr)
	if dbBaseURL == "" {
		dbBaseURL = "http://db:8083"
	}

	saveInitialData(dbBaseURL)

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		if os.Getenv(confHealthFailure) == "true" {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("FAILURE"))
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("OK"))
		}
	})

	http.HandleFunc("/api/v1/some-data", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "Key required", http.StatusBadRequest)
			return
		}

		resp, err := http.Get(dbBaseURL + "/db/" + key)
		if err != nil {
			http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusNotFound {
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if resp.StatusCode != http.StatusOK {
			http.Error(w, "DB error", http.StatusInternalServerError)
			return
		}

		var data map[string]string
		if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
			http.Error(w, "Error decoding DB response", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(data)
	})

	server := httptools.CreateServer(*port, nil)
	server.Start()
	signal.WaitForTerminationSignal()
}
