package linkdb

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

type APIRequest struct {
	Domain *string `json:"domain,omitempty"`
	Limit  *int64  `json:"limit,omitempty"`
	Page   *int64  `json:"page,omitempty"`
}

type ApiError struct {
	ErrorCode string `json:"errorCode"`
	Function  string `json:"function"`
	Error     string `json:"error"`
}