# GlobalLinks Project

GlobalLinks is a link gathering tool based on the Common Crawl dataset. It's currently in alpha and under active development.

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

### Output

links files are stored in data/links/

pages files are stored in data/pages/

### Format

link: linkedDomain|linkedSubdomain|linkedPath|linkedQuery|linkedScheme|sourceHost|sourcePath|sourceQuery|sourceScheme|linkText|nofollow|noindex|date_imported|ip

page: sourceHost|sourcePath|sourceQuery|sourceScheme|pageTitle|ip|date_imported|internal_links_qty|external_links_qty|noindex


## System Requirements
- Go 1.21 or later.
- Requires 1.5GB of RAM per thread.
- Minimum 40GB of free disk for every segment parsed.

## Alpha Version Disclaimer
This is an alpha version of GlobalLinks and is subject to changes. The software is provided "as is", without warranty of any kind.

