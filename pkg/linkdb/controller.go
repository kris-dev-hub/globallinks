package linkdb

import (
	"context"
	"log"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (app *App) ControllerGetDomainLinks(apiRequest APIRequest) ([]LinkOut, error) {
	var links []LinkRow
	var outLinks []LinkOut
	var limit int64 = 100
	var page int64 = 1

	domain := *apiRequest.Domain
	if apiRequest.Limit != nil && *apiRequest.Limit > 0 && *apiRequest.Limit <= 1000 {
		limit = *apiRequest.Limit
	}
	if apiRequest.Page != nil && *apiRequest.Page > 0 {
		page = *apiRequest.Page
	}

	// Get the collection
	collection := app.DB.Database(app.Dbname).Collection("links")

	// Create a filter for the query
	filter := bson.M{"linkdomain": domain}
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

	findOptions := options.Find().SetSort(sort).SetLimit(limit).SetSkip((page - 1) * limit)

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

	outLinks = cleanDomainLinks(&links)

	return outLinks, nil
}

func cleanDomainLinks(links *[]LinkRow) []LinkOut {
	lastLink := LinkOut{}
	curLink := LinkOut{}
	outLinks := make([]LinkOut, 0, len(*links))
	for _, link := range *links {
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
