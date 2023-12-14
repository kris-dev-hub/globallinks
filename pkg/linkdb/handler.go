package linkdb

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/kris-dev-hub/globallinks/pkg/commoncrawl"
)

// SendResponse - send http response
func SendResponse(w http.ResponseWriter, status int, data []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if _, err := w.Write(data); err != nil {
		log.Printf("error writing response: %v", err)
	}
}

// HandlerGetDomainLinks - get domain links
func (app *App) HandlerGetDomainLinks(w http.ResponseWriter, r *http.Request) {
	var apiRequest APIRequest
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	err := decoder.Decode(&apiRequest)
	if err != nil {
		errorMsg := fmt.Sprintf("Error parsing request: %s", err)
		SendResponse(w, http.StatusBadRequest, GenerateError("ErrorParsing", "HandlerGetDomainLinks", errorMsg))
		return
	}

	if apiRequest.Domain == nil || *apiRequest.Domain == "" {
		SendResponse(w, http.StatusBadRequest, GenerateError("ErrorNoDomain", "HandlerGetDomainLinks", "Domain is required"))
		return
	}

	if !commoncrawl.IsValidDomain(*apiRequest.Domain) {
		SendResponse(w, http.StatusBadRequest, GenerateError("ErrorInvalidDomain", "HandlerGetDomainLinks", "Invalid domain"))
		return
	}

	links, err := app.ControllerGetDomainLinks(apiRequest)
	if err != nil {
		SendResponse(w, http.StatusInternalServerError, GenerateError("ErrorFailedLinks", "HandlerGetDomainLinks", "Error getting links"))
		return
	}

	response, err := json.Marshal(links)
	if err != nil {
		SendResponse(w, http.StatusInternalServerError, GenerateError("ErrorJson", "HandlerGetDomainLinks", "Error marshalling links"))
		return
	}

	SendResponse(w, http.StatusOK, response)
}