# GlobalLinks Project

GlobalLinks is a backlink gathering tool based on the Common Crawl dataset. It's currently in alpha and under active development.

## Features

- Multithreaded processing of links.
- Parses up to 300,000 pages per minute per thread.

## Configuration

Control the number of threads with the `GLOBALLINKS_MAXTHREADS` environment variable:

```sh
export GLOBALLINKS_MAXTHREADS=4
```

Control the number of WAT files parsed in one go `GLOBALLINKS_MAXWATFILES` environment variable:

```sh
export GLOBALLINKS_MAXWATFILES=10
```

Set path for data files , default "data" `GLOBALLINKS_DATAPATH` environment variable:

```sh
export GLOBALLINKS_DATAPATH=data
```

## Usage
Start by selecting an archive and its segment name from Common Crawl https://www.commoncrawl.org/get-started. Then run the following command:

```sh
go run cmd/importer/main.go CC-MAIN-2021-04
```

Replace CC-MAIN-2021-04 with your chosen archive name and 20210115134101 with the segment name.

Distributing backlinks data into tree directory structure to be able to build API on top of it.

```sh
go run cmd/storelinks/main.go data/links/compact_0.txt.gz data/linkdb

```

Replace data/links/compact_0.txt.gz with your chosen compacted links file and data/linkdb with your chosen output directory.
Repeating this command for all compacted segment links files will update the tree directory structure in data/linkdb.



## Test settings

wat.go file contains line "const debugTestMode = false". Setting it to true import only 10 files from 3 segments. Allow to watch whole process on limited data. It will use only 30 files for test and not 90000.

### Output

links files are stored in data/links/

pages files are stored in data/pages/

### Format

link: linkedDomain|linkedSubdomain|linkedPath|linkedQuery|linkedScheme|sourceHost|sourcePath|sourceQuery|sourceScheme|linkText|nofollow|noindex|date_imported|ip

page: sourceHost|sourcePath|sourceQuery|sourceScheme|pageTitle|ip|date_imported|internal_links_qty|external_links_qty|noindex

## Docker compose
Build the docker image, and collect the data from the archive CC-MAIN-2021-04 for 6 files and 4 threads.
There are 90000 files to collect in one archive, 720 files for one segment.

```sh
make compose-up ARCHIVENAME="CC-MAIN-2021-04" GLOBALLINKS_MAXWATFILES=6 GLOBALLINKS_MAXTHREADS=4
```

Data will be stored in watdata directory, you can restart the process multiple times it will continue from the last file.

## Docker

Build the docker image.

```sh
make docker-build
```

Example how to use docker image to collect data from the archive CC-MAIN-2021-04 for 6 files and 4 threads.

```sh
ARCHIVENAME='CC-MAIN-2021-04' GLOBALLINKS_MAXWATFILES='6' GLOBALLINKS_MAXTHREADS='4' make docker-start
```

Data will be also stored in watdata directory.

Stop running docker image.

```sh
make docker-stop
```

## Docker Image Availability
The Docker image is available on Docker Hub.

### Parameters Description
- **Archive Name:** `CC-MAIN-2021-04` - Name of the archive to be parsed.
- **Number of Files:** `4` - Number of files to be parsed. Currently, there are 90,000 files in one archive, with 900 in each segment. Parsing at least one segment is necessary to obtain compacted results.
- **Number of Threads:** `2` - Number of threads to use (ranging from 1 to 16).

### Resource Utilization and Performance
- **Memory Usage:** One tread typically consumes approximately 1.5 GB of RAM. Therefore, running 4 threads will require about 6 GB of RAM.
- **Processing Time:** The time taken to process one segment varies depending on CPU and network speed. It generally takes a few hours.

### Data Storage
Data will be stored in the `watdata` directory:
- `links`: Contains the final parsed segment links.
- `pages`: Includes the final parsed segment pages.
- `tmp/links`: Temporary storage for parsed segment link files.
- `tmp/pages`: Temporary storage for parsed segment page files.
- `tmp/wat`: Temporary storage for downloaded WAT files to be parsed.

### Example
```sh
docker pull krisdevhub/globallinks:latest   
docker run --name globallinks-test -d -v ./watdata:/app/data krisdevhub/globallinks:latest /app/importer CC-MAIN-2021-04 4 2
```

At the end you can also set number of segments you want to import. Range from 0 to 99. Format 2,3,4,5 or 2-5 is accepted.

### Data

example record from link file:

```sh
blogmedyczny.edu.pl||/czasopisma-kobiece-i-tabletki-na-odchudzanie/||2|turysta24.pl|/tabletki-odchudzajace-moga-pomoc-zredukowac-wage/||2|turysta24.pl|/tabletki-odchudzajace-moga-pomoc-zredukowac-wage/||2|Theme Palace|0|0|2023-02-04|51.75.43.178

LinkedDomain|LinkedSubdomain|LinkedPath|LinkedQuery|LinkedScheme|PageHost|PagePath|PageQuery|PageScheme|LinkText|NoFollow|NoIndex|DateImported|IP
```

There are around 6 billion unique external backlinks per month in the common crawl data and the application is able to analyse and collect them all.

## System Requirements
- Go 1.21 or later.
- Requires 1.5GB of RAM per thread.
- Minimum 40GB of free disk for every segment parsed.

## Alpha Version Disclaimer
This is an alpha version of GlobalLinks and is subject to changes. The software is provided "as is", without warranty of any kind.

