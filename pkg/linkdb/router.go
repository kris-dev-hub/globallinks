package linkdb

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/kris-dev-hub/globallinks/pkg/healthcheck"
)

func InitRoutes(app *App) *mux.Router {
	router := mux.NewRouter()
	router = app.LinkdbApiRoutes(router)
	return router
}

func (app *App) LinkdbApiRoutes(router *mux.Router) *mux.Router {
	// swagger:route GET /health health HealthResponse
	// Returns a health check
	// responses:
	//   200:
	//   500:
	router.HandleFunc("/api/health", healthcheck.HealthResponse).Methods(http.MethodGet)
	// swagger:route POST /api/transaction transactions AddTransaction
	// Adds a transaction
	// responses:
	//   200: Transaction Response on success
	//   400: Bad Request
	//   500:
	router.HandleFunc("/api/links", app.HandlerGetDomainLinks).Methods(http.MethodPost)
	return router
}
