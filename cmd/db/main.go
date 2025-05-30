package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"strings"

	"github.com/ypapish/software-architecture-lab5/datastore"
	"github.com/ypapish/software-architecture-lab5/httptools"
	"github.com/ypapish/software-architecture-lab5/signal"
)

var port = flag.Int("port", 8081, "server port")

func main() {
	flag.Parse()

	db, err := datastore.Open("db_data")
	if err != nil {
		log.Fatal("Error opening database:", err)
	}
	defer db.Close()

	http.HandleFunc("/db/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/db/")
		if key == "" {
			http.Error(w, "Key required", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodGet:
			value, err := db.Get(key)
			if err != nil {
				if err == datastore.ErrNotFound {
					http.NotFound(w, r)
				} else {
					http.Error(w, "DB error", http.StatusInternalServerError)
				}
				return
			}
			response := map[string]string{"key": key, "value": value}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(response)

		case http.MethodPost:
			var data struct{ Value string }
			if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
				http.Error(w, "Invalid JSON", http.StatusBadRequest)
				return
			}
			defer r.Body.Close()

			if err := db.Put(key, data.Value); err != nil {
				http.Error(w, "DB error", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusCreated)

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	server := httptools.CreateServer(*port, nil)
	server.Start()
	signal.WaitForTerminationSignal()
}
