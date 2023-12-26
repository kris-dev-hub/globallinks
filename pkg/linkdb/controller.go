package linkdb

import (
	"context"
	"log"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/net/publicsuffix"
)

const (
	FilterKindExact = "exact"
	FilterKindAny   = "any"
)

func (app *App) ControllerGetDomainLinks(apiRequest APIRequest) ([]LinkOut, error) {
	var links []LinkRow
	var outLinks []LinkOut
	var limit int64 = 100
	var page int64 = 1

	domain := *apiRequest.Domain
	if apiRequest.Limit != nil && *apiRequest.Limit > 0 && *apiRequest.Limit <= 100 {
		limit = *apiRequest.Limit
	}
	if apiRequest.Page != nil && *apiRequest.Page > 0 {
		page = *apiRequest.Page
	}

	// Get the collection
	collection := app.DB.Database(app.Dbname).Collection("links")

	domainParsed, err := publicsuffix.EffectiveTLDPlusOne(domain)
	if err != nil {
		return nil, err
	}

	filter := generateFilter(domain, domainParsed, &apiRequest)

	sort := bson.D{
		{Key: "linkdomain", Value: 1},
		{Key: "linkpath", Value: 1},
		{Key: "linkrawquery", Value: 1},
		{Key: "pagehost", Value: 1},
		{Key: "pagepath", Value: 1},
		{Key: "pagerawquery", Value: 1},
		{Key: "datefrom", Value: 1},
		{Key: "dateto", Value: 1},
	}

	sortValue := 1
	if apiRequest.Order != nil {
		switch *apiRequest.Order {
		case "desc":
			sortValue = -1
		}
	}

	if apiRequest.Sort != nil {
		switch *apiRequest.Sort {
		case "linkUrl":
			sort = bson.D{
				{Key: "linkdomain", Value: sortValue},
				{Key: "linkpath", Value: sortValue},
				{Key: "linkrawquery", Value: sortValue},
			}
		case "pageUrl":
			sort = bson.D{
				{Key: "pagehost", Value: sortValue},
				{Key: "pagepath", Value: sortValue},
				{Key: "pagerawquery", Value: sortValue},
			}
		case "linkText":
			sort = bson.D{
				{Key: "linktext", Value: sortValue},
			}
		case "dateFrom":
			sort = bson.D{
				{Key: "datefrom", Value: sortValue},
			}
		case "dateTo":
			sort = bson.D{
				{Key: "dateto", Value: sortValue},
			}
		}
	}

	// take more pages since we can have duplicates
	findOptions := options.Find().SetSort(sort).SetLimit(limit * 3).SetSkip((page - 1) * limit)

	cursor, err := collection.Find(context.TODO(), filter, findOptions)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(context.TODO())

	// Iterate through the cursor
	for cursor.Next(context.TODO()) {
		var link LinkRow
		if err := cursor.Decode(&link); err != nil {
			return nil, err
		}
		links = append(links, link)
	}

	if err := cursor.Err(); err != nil {
		return nil, err
	}

	outLinks = cleanDomainLinks(&links, limit)

	return outLinks, nil
}

// generateFilter creates a MongoDB filter based on the given parameters
func generateFilter(domain string, domainParsed string, apiRequest *APIRequest) bson.M {
	// Create a filter for the query
	filter := bson.M{"linkdomain": domain}
	if domainParsed != domain {
		subdomain := domain[:len(domain)-len(domainParsed)-1]
		filter = bson.M{"linkdomain": domainParsed, "linksubdomain": subdomain}
	}
	if apiRequest.Filters != nil {
		for _, filterData := range *apiRequest.Filters {
			switch filterData.Name {
			case "No Follow":
				val, err := strconv.Atoi(filterData.Val)
				if err == nil {
					filter["nofollow"] = val
				}
			case "Link Path":
				if filterData.Kind == FilterKindExact {
					filter["linkpath"] = bson.M{"$regex": primitive.Regex{Pattern: "^" + filterData.Val + "$", Options: "i"}}
				}
				if filterData.Kind == FilterKindAny {
					filter["linkpath"] = bson.M{"$regex": primitive.Regex{Pattern: filterData.Val, Options: "i"}}
				}
			case "Source Host":
				if filterData.Kind == FilterKindExact {
					filter["pagehost"] = bson.M{"$regex": primitive.Regex{Pattern: "^" + filterData.Val + "$", Options: "i"}}
				}
				if filterData.Kind == FilterKindAny {
					filter["pagehost"] = bson.M{"$regex": primitive.Regex{Pattern: filterData.Val, Options: "i"}}
				}
			case "Source Path":
				if filterData.Kind == FilterKindExact {
					filter["pagepath"] = bson.M{"$regex": primitive.Regex{Pattern: "^" + filterData.Val + "$", Options: "i"}}
				}
				if filterData.Kind == FilterKindAny {
					filter["pagepath"] = bson.M{"$regex": primitive.Regex{Pattern: filterData.Val, Options: "i"}}
				}
			case "Anchor":
				if filterData.Kind == FilterKindExact {
					filter["linktext"] = bson.M{"$regex": primitive.Regex{Pattern: "^" + filterData.Val + "$", Options: "i"}}
				}
				if filterData.Kind == FilterKindAny {
					filter["linktext"] = bson.M{"$regex": primitive.Regex{Pattern: filterData.Val, Options: "i"}}
				}

			}
		}
	}

	return filter
}

func cleanDomainLinks(links *[]LinkRow, limit int64) []LinkOut {
	lastLink := LinkOut{}
	curLink := LinkOut{}
	outLinks := make([]LinkOut, 0, len(*links))
	i := 0
	for _, link := range *links {

		if i >= int(limit) {
			break
		}

		curLink = LinkOut{
			LinkUrl:  showLinkScheme(link.LinkScheme) + "://" + showSubDomain(link.LinkSubDomain) + link.LinkDomain + showLinkPath(link.LinkPath) + showSubQuery(link.LinkRawQuery),
			PageUrl:  showLinkScheme(link.PageScheme) + "://" + link.PageHost + showLinkPath(link.PagePath) + showSubQuery(link.PageRawQuery),
			LinkText: link.LinkText,
			NoFollow: link.NoFollow,
			NoIndex:  link.NoIndex,
			DateFrom: link.DateFrom,
			DateTo:   link.DateTo,
			IP:       []string{link.IP},
			Qty:      link.Qty,
		}

		if lastLink.LinkUrl != curLink.LinkUrl || lastLink.PageUrl != curLink.PageUrl || lastLink.LinkText != curLink.LinkText || lastLink.NoFollow != curLink.NoFollow {
			if lastLink.LinkUrl != "" {
				outLinks = append(outLinks, lastLink)
				i++
			}
			lastLink = curLink
			continue
		}

		if lastLink.DateFrom < curLink.DateFrom {
			lastLink.DateFrom = curLink.DateFrom
		}

		if lastLink.DateTo > curLink.DateTo {
			lastLink.DateTo = curLink.DateTo
		}

		addIPsToLink(&lastLink, &curLink)

		lastLink.Qty += curLink.Qty

	}

	return outLinks
}

func showLinkScheme(scheme string) string {
	if scheme == "1" {
		return "http"
	}
	return "https"
}

func showSubDomain(subDomain string) string {
	if subDomain == "" {
		return ""
	}
	return subDomain + "."
}

func showSubQuery(rawQuery string) string {
	if rawQuery == "" {
		return ""
	}
	return "?" + rawQuery
}

func showLinkPath(linkPath string) string {
	if linkPath == "" {
		return "/"
	}
	return linkPath
}

func addIPsToLink(lastLink *LinkOut, curLink *LinkOut) {
	alreadyExists := false
	for _, ip := range lastLink.IP {
		if ip == curLink.IP[0] {
			alreadyExists = true
			break
		}
	}

	// If it's not already in the slice, append it
	if !alreadyExists {
		lastLink.IP = append(lastLink.IP, curLink.IP[0])
	}
}

func (app *App) isRateLimited(identifier string) bool {
	const limit = 50
	const windowDuration = 15 * time.Minute

	now := time.Now()

	// Check if the user has made a request before
	if info, exists := app.requestRecords[identifier]; exists {
		// Check if the window duration has passed
		if now.Sub(info.FirstRequestTime) > windowDuration {
			// Reset the counter
			info.FirstRequestTime = now
			info.RequestCount = 1
			return false
		} else {
			// Increment the counter
			info.RequestCount++
			// Check if the request limit is exceeded
			return info.RequestCount > limit
		}
	} else {
		// First request from this user
		app.requestRecords[identifier] = &RequestInfo{FirstRequestTime: now, RequestCount: 1}
		return false
	}
}
