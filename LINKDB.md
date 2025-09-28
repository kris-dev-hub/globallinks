# LinkDB API Documentation

## Overview

LinkDB API provides access to link data extracted from Common Crawl archives. The API allows you to search for backlinks to specific domains and filter results based on various criteria.

## Base URL

- **Development:** `http://localhost:8010`
- **Production:** `https://localhost:8443`
- **Docker:** `http://localhost:8010` (or custom port mapping)

## Rate Limiting

- **Limit:** 50 requests per 15-minute window per IP address
- **Response:** HTTP 429 when rate limit exceeded

## Authentication

No authentication required.

## CORS Policy

- **Access-Control-Allow-Origin:** `*` (all origins allowed)
- **Methods:** `POST, GET, OPTIONS, PUT, DELETE`
- **Headers:** `Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization`

---

## Endpoints

### 1. Health Check

**Endpoint:** `GET /api/health`

Check if the API service is running and healthy.

#### Example Request
```bash
curl -X GET http://localhost:8010/api/health
```

#### Response
Returns health status information.

---

### 2. Get Domain Links

**Endpoint:** `POST /api/links`

Retrieve backlinks for a specific domain from Common Crawl data.

#### Request Body

```json
{
  "domain": "example.com",
  "limit": 50,
  "page": 1,
  "sort": "linkUrl",
  "order": "asc",
  "filters": [
    {
      "name": "No Follow",
      "val": "0",
      "kind": "exact"
    }
  ]
}
```

#### Parameters

| Parameter | Type | Required | Description | Default | Constraints |
|-----------|------|----------|-------------|---------|-------------|
| `domain` | string | **Yes** | Target domain to search for backlinks | - | Accepts `domain.com` or `http://domain.com` |
| `limit` | integer | No | Number of results per page | 100 | 1-100 |
| `page` | integer | No | Page number for pagination | 1 | > 0 |
| `sort` | string | No | Field to sort results by | Multiple fields | See sort options below |
| `order` | string | No | Sort order | `asc` | `asc`, `desc` |
| `filters` | array | No | Array of filter objects | - | See filters section below |

#### Sort Options

| Sort Value | Description | Database Fields |
|------------|-------------|-----------------|
| `linkUrl` | Sort by target link URL | `linkdomain`, `linkpath`, `linkrawquery` |
| `pageUrl` | Sort by source page URL | `pagehost`, `pagepath`, `pagerawquery` |
| `linkText` | Sort by anchor text | `linktext` |
| `dateFrom` | Sort by start date | `datefrom` |
| `dateTo` | Sort by end date | `dateto` |

#### Filters

Filters allow you to narrow down search results based on specific criteria.

##### Filter Structure
```json
{
  "name": "Filter Name",
  "val": "filter value",
  "kind": "exact|any"
}
```

##### Available Filters

| Filter Name | Description | Kind Options | Example Values |
|-------------|-------------|--------------|----------------|
| `No Follow` | Filter by nofollow attribute | `exact` only | `"0"` (follow), `"1"` (nofollow) |
| `Link Path` | Filter by target link path | `exact`, `any` | `"/page"`, `"/blog/*"` |
| `Source Host` | Filter by source page hostname | `exact`, `any` | `"source.com"`, `"*.example.com"` |
| `Source Path` | Filter by source page path | `exact`, `any` | `"/articles"`, `"/blog/*"` |
| `Anchor` | Filter by anchor text | `exact`, `any` | `"click here"`, `"read more"` |
| `IP` | Filter by IP address | `exact`, `any` | `"192.168.1.1"`, `"192.168.*"` |

##### Filter Kinds

- **`exact`**: Exact match (case-insensitive)
- **`any`**: Pattern match (case-insensitive, supports partial matching)

#### Example Requests

##### Basic Request
```bash
curl -X POST http://localhost:8010/api/links \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "example.com"
  }'
```

##### Advanced Request with Filters
```bash
curl -X POST http://localhost:8010/api/links \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "example.com",
    "limit": 25,
    "page": 1,
    "sort": "linkUrl",
    "order": "desc",
    "filters": [
      {
        "name": "No Follow",
        "val": "0",
        "kind": "exact"
      },
      {
        "name": "Anchor",
        "val": "click",
        "kind": "any"
      },
      {
        "name": "Source Host",
        "val": "blog.example.com",
        "kind": "exact"
      }
    ]
  }'
```

#### Response Format

##### Success Response (HTTP 200)
```json
[
  {
    "link_url": "https://example.com/products/item1",
    "page_url": "https://source.com/blog/review",
    "link_text": "Check out this product",
    "no_follow": 0,
    "no_index": 0,
    "date_from": "2023-01-15",
    "date_to": "2023-12-30",
    "ip": ["192.168.1.1", "10.0.0.1"],
    "qty": 3
  },
  {
    "link_url": "https://example.com/about",
    "page_url": "https://partner.com/links",
    "link_text": "About Example Company",
    "no_follow": 1,
    "no_index": 0,
    "date_from": "2023-03-01",
    "date_to": "2023-11-15",
    "ip": ["203.0.113.1"],
    "qty": 1
  }
]
```

##### Response Fields

| Field | Type | Description |
|-------|------|-------------|
| `link_url` | string | Complete URL of the target link |
| `page_url` | string | Complete URL of the source page containing the link |
| `link_text` | string | Anchor text of the link |
| `no_follow` | integer | NoFollow attribute (0 = follow, 1 = nofollow) |
| `no_index` | integer | NoIndex attribute (0 = index, 1 = noindex) |
| `date_from` | string | Earliest date when link was found |
| `date_to` | string | Latest date when link was found |
| `ip` | array | IP addresses where the link was found |
| `qty` | integer | Number of times this link was found |

## Error Handling

### Error Response Format
```json
{
  "errorCode": "ErrorInvalidDomain",
  "function": "HandlerGetDomainLinks",
  "error": "Invalid domain"
}
```

### Error Codes

| Error Code | HTTP Status | Description |
|------------|-------------|-------------|
| `ErrorTooManyRequests` | 429 | Rate limit exceeded (50 requests per 15 minutes) |
| `ErrorParsing` | 400 | Invalid JSON format or domain parsing error |
| `ErrorNoDomain` | 400 | Missing required domain parameter |
| `ErrorInvalidDomain` | 400 | Domain format is invalid |
| `ErrorFailedLinks` | 500 | Database query failed |
| `ErrorJson` | 500 | JSON marshalling error |

## Query Limitations

- **Timeout:** 60 seconds per query
- **Max Results:** 100 per page
- **Pagination:** Use `page` parameter for additional results
- **Domain Validation:** Domains must pass public suffix validation

## Data Processing

- **Deduplication:** Results are automatically deduplicated based on link URL, page URL, link text, and nofollow status
- **IP Aggregation:** Multiple IP addresses for the same link are combined
- **Date Range:** Date ranges are merged when combining duplicate entries
- **Quantity Summation:** Quantities are summed when merging duplicate entries

## Examples

### Find All Backlinks to a Domain
```bash
curl -X POST http://localhost:8010/api/links \
  -H "Content-Type: application/json" \
  -d '{"domain": "mysite.com"}'
```

### Find Only DoFollow Links
```bash
curl -X POST http://localhost:8010/api/links \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "mysite.com",
    "filters": [
      {
        "name": "No Follow",
        "val": "0",
        "kind": "exact"
      }
    ]
  }'
```

### Find Links with Specific Anchor Text
```bash
curl -X POST http://localhost:8010/api/links \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "mysite.com",
    "filters": [
      {
        "name": "Anchor",
        "val": "best product",
        "kind": "any"
      }
    ]
  }'
```

### Find Links from Specific Source Domain
```bash
curl -X POST http://localhost:8010/api/links \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "mysite.com",
    "filters": [
      {
        "name": "Source Host",
        "val": "authoritative-site.com",
        "kind": "exact"
      }
    ]
  }'
```

### Find Links from Specific IP Address
```bash
curl -X POST http://localhost:8010/api/links \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "mysite.com",
    "filters": [
      {
        "name": "IP",
        "val": "192.168.1.1",
        "kind": "exact"
      }
    ]
  }'
```

### Find Links from IP Range
```bash
curl -X POST http://localhost:8010/api/links \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "mysite.com",
    "filters": [
      {
        "name": "IP",
        "val": "192.168.",
        "kind": "any"
      }
    ]
  }'
```

### Paginated Results with Sorting
```bash
curl -X POST http://localhost:8010/api/links \
  -H "Content-Type: application/json" \
  -d '{
    "domain": "mysite.com",
    "limit": 50,
    "page": 2,
    "sort": "dateFrom",
    "order": "desc"
  }'
```

---

## 🐳 Docker Deployment

The LinkDB API is available as a containerized solution for easy deployment and scaling.

### Quick Start with Docker

**1. Pull the image:**
```bash
docker pull ghcr.io/kris-dev-hub/globallinks-linksapi:latest
```

**2. Run without authentication:**
```bash
docker run -d \
  --name linksapi \
  -p 8010:8010 \
  -e MONGO_HOST=your_mongo_host \
  -e MONGO_DATABASE=linksdb \
  ghcr.io/kris-dev-hub/globallinks-linksapi:latest
```

**3. Run with MongoDB authentication:**
```bash
docker run -d \
  --name linksapi \
  -p 8010:8010 \
  -e MONGO_HOST=your_mongo_host \
  -e MONGO_USERNAME=your_username \
  -e MONGO_PASSWORD=your_password \
  -e MONGO_DATABASE=linksdb \
  -e MONGO_AUTH_DB=linksdb \
  ghcr.io/kris-dev-hub/globallinks-linksapi:latest
```

### Environment Variables

| Variable | Description | Default | Example |
|----------|-------------|---------|---------|
| `MONGO_HOST` | MongoDB hostname | localhost | `mongodb.example.com` |
| `MONGO_PORT` | MongoDB port | 27017 | `27017` |
| `MONGO_DATABASE` | Database name | linkdb | `linkdb` |
| `MONGO_USERNAME` | MongoDB username | - | `admin` |
| `MONGO_PASSWORD` | MongoDB password | - | `password123` |
| `MONGO_AUTH_DB` | Authentication database | admin | `admin` |
| `GO_ENV` | Environment mode | development | `production` |

### External MongoDB Connection

The LinkDB API is designed to connect to existing MongoDB instances without bundling MongoDB in the container:

**Connect to local MongoDB:**
```bash
docker run -d \
  --name linksapi \
  -p 8010:8010 \
  -e MONGO_HOST=host.docker.internal \
  -e MONGO_USERNAME=admin \
  -e MONGO_PASSWORD=password123 \
  -e MONGO_DATABASE=linkdb \
  ghcr.io/kris-dev-hub/globallinks-linksapi:latest
```

**Connect to remote MongoDB:**
```bash
docker run -d \
  --name linksapi \
  -p 8010:8010 \
  -e MONGO_HOST=192.168.1.105 \
  -e MONGO_USERNAME=linksuser \
  -e MONGO_PASSWORD=secret_pass \
  -e MONGO_DATABASE=linksdb \
  -e MONGO_AUTH_DB=linksdb \
  ghcr.io/kris-dev-hub/globallinks-linksapi:latest
```

### Production Deployment

**With HTTPS (Production mode):**
```bash
docker run -d \
  --name linksapi-prod \
  -p 8443:8443 \
  -v /path/to/ssl/certs:/app/cert \
  -e GO_ENV=production \
  -e MONGO_HOST=your_mongo_host \
  -e MONGO_USERNAME=your_username \
  -e MONGO_PASSWORD=your_password \
  -e MONGO_DATABASE=linkdb \
  ghcr.io/kris-dev-hub/globallinks-linksapi:latest
```

**Required SSL certificates for production:**
- `/app/cert/fullchain.pem` - SSL certificate
- `/app/cert/privkey.pem` - Private key

### Container Features

- **Lightweight**: ~15-25MB Alpine-based image
- **Secure**: Non-root user execution
- **Health checks**: Built-in monitoring
- **Multi-architecture**: AMD64 support
- **Environment-driven**: Full configuration via environment variables

### Health Check

The container includes a built-in health check:
```bash
# Check container health
docker ps --format "table {{.Names}}\t{{.Status}}"

# Manual health check
curl http://localhost:8010/api/health
```

### Logging

**View container logs:**
```bash
# Real-time logs
docker logs -f linksapi

# Last 100 lines
docker logs --tail 100 linksapi
```

### Scaling

**Multiple instances with load balancer:**
```bash
# Start multiple instances
docker run -d --name linksapi-1 -p 8011:8010 -e MONGO_HOST=mongodb ghcr.io/kris-dev-hub/globallinks-linksapi:latest
docker run -d --name linksapi-2 -p 8012:8010 -e MONGO_HOST=mongodb ghcr.io/kris-dev-hub/globallinks-linksapi:latest
docker run -d --name linksapi-3 -p 8013:8010 -e MONGO_HOST=mongodb ghcr.io/kris-dev-hub/globallinks-linksapi:latest
```

---