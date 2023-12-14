package linkdb

import "encoding/json"

// GenerateError - generate error response
func GenerateError(errorCode string, errorFunction string, errorInfo string) []byte {
	errorData := new(ApiError)
	errorData.ErrorCode = errorCode
	errorData.Function = errorFunction
	errorData.Error = errorInfo
	jsonError, _ := json.Marshal(errorData)
	return jsonError
}
