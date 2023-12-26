package linkdb

import (
	"time"
)

// LinkRow - link row
type LinkRow struct {
	LinkDomain    string `json:"link_domain"`
	LinkSubDomain string `json:"link_sub_domain"`
	LinkPath      string `json:"link_path"`
	LinkRawQuery  string `json:"link_raw_query"`
	LinkScheme    string `json:"link_scheme"`
	PageHost      string `json:"page_host"`
	PagePath      string `json:"page_path"`
	PageRawQuery  string `json:"page_raw_query"`
	PageScheme    string `json:"page_scheme"`
	LinkText      string `json:"link_text"`
	NoFollow      int    `json:"no_follow"`
	NoIndex       int    `json:"no_index"`
	DateFrom      string `json:"date_from"`
	DateTo        string `json:"date_to"`
	IP            string `json:"ip"`
	Qty           int    `json:"qty"`
}

// LinkOut - link output
type LinkOut struct {
	LinkUrl  string   `json:"link_url"`
	PageUrl  string   `json:"page_url"`
	LinkText string   `json:"link_text"`
	NoFollow int      `json:"no_follow"`
	NoIndex  int      `json:"no_index"`
	DateFrom string   `json:"date_from"`
	DateTo   string   `json:"date_to"`
	IP       []string `json:"ip"`
	Qty      int      `json:"qty"`
}

type ApiRequestFilter struct {
	Name string `json:"name"`
	Val  string `json:"val"`
	Kind string `json:"kind"`
}

type APIRequest struct {
	Domain  *string             `json:"domain,omitempty"`
	Limit   *int64              `json:"limit,omitempty"`
	Sort    *string             `json:"sort,omitempty"`
	Order   *string             `json:"order,omitempty"`
	Page    *int64              `json:"page,omitempty"`
	Filters *[]ApiRequestFilter `json:"filters,omitempty"`
	/*
		NoFollow  *int    `json:"no_follow,omitempty"`
		TextExact *string `json:"text_exact,omitempty"`
		TextAny   *string `json:"text_any,omitempty"`
	*/
}

type ApiError struct {
	ErrorCode string `json:"errorCode"`
	Function  string `json:"function"`
	Error     string `json:"error"`
}

// RequestInfo - request info used to count requests in a period of time
type RequestInfo struct {
	FirstRequestTime time.Time
	RequestCount     int
}
