package healthcheck

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func InitRoutes() *mux.Router {
	router := mux.NewRouter()
	router.HandleFunc("/health", HealthResponse).Methods(http.MethodGet)
	return router
}

func HealthResponse(w http.ResponseWriter, r *http.Request) {
	_, err := w.Write([]byte("I am alive!"))
	if err != nil {
		// Log the error instead of panicking
		log.Printf("Error writing response: %v", err)

		// Send an HTTP error response
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}
