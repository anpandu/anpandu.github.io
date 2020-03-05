---
layout: single
classes: wide
title:  "Inserting 1 Million JSON data into BigQuery Table"
# cover: "https://pbs.twimg.com/profile_banners/91470567/1519807645/1500x500"
description: An example post which shows code rendering.
date:   2019-05-23 21:03:36 +0530
categories:
  - Programming
tags:
  - golang
  - bigquery
# toc: true
excerpt: "This post should [...]"
# header:
#   overlay_image: "https://pbs.twimg.com/profile_banners/91470567/1519807645/1500x500"
#   overlay_filter: 0.5 # same as adding an opacity of 0.5 to a black background
#   teaser: "https://pbs.twimg.com/profile_banners/91470567/1519807645/1500x500"
  # caption: "Photo credit: [**Unsplash**](https://unsplash.com)"
  # actions:
  #   - label: "More Info"
  #     url: "https://unsplash.com"
share: False
related: False
---

Moving data from multiple sources into multiple databases is one of the data engineer's responsibilities. It is always recommended to use simple and direct approach when the data is small enough, like iterating data one by one and inserting it at the same time. However this approach will not going to work well on larger sized data or higher throughput stream data.

In this blog post, we will demonstrate how to insert JSON text file into Google's BigQuery Table using Go language. Go is known for one of the best language to write high-performance programs due to it's features and native libraries to write concurrent and parallel programming easier.

### Preparation

For preparation, let's create a text file containing 1 million rows of single JSON per row, using a python script provided below [1]. We will get a text file like this.

```json
{"id":1,"name":"aaa"}
{"id":2,"name":"bbb"}
...
```

For BigQuery, make sure we have GCP key JSON file that has access to modify BigQuery table and don't forget to export it to `GOOGLE_APPLICATION_CREDENTIALS` env var. After that, we all set.

### Simple approach

We will do three approach to do insertion. But first, let's use simplest approach. We will read text file one by one and insert it to a BigQuery table one at a time. Let's design the architecture.

<figure>
	<a href="http://farm9.staticflickr.com/8426/7758832526_cc8f681e48_b.jpg"><img src="http://farm9.staticflickr.com/8426/7758832526_cc8f681e48_c.jpg"></a>
	<!-- <figcaption><a href="http://www.flickr.com/photos/80901381@N04/7758832526/" title="Morning Fog Emerging From Trees by A Guy Taking Pictures, on Flickr">Morning Fog Emerging From Trees by A Guy Taking Pictures, on Flickr</a>.</figcaption> -->
</figure>

In this diagram, there are three main components: 1) `File Reader`, 2) Channel `c1`, and 3) `Worker`. `File Reader` will read text file one by one line and send it to channel `c1`. Channel `c1` to share string data. `Worker` is responsible for receiving string data from channel `c1` and inserting it into a BigQuery table. 

Channel is a native type in Go that enable us to send/receive values and share it accross coroutine (goroutine). Let's define Channel `c1` first.

```go
c1 = make(chan string)
```

Next, we create `File Reader` as an anonymous function and immediately run it as a goroutine. Using `go` keyword in front of function call enable it to run asynchrounously/non-blocking to main function.

```go
// Read file line by line and send it to channel
log.Info("Reading file")
go func(filepath string, chanStr chan<- string) {
    file, _ := os.Open(filepath)
    defer file.Close()
    scanner := bufio.NewScanner(file)
    counter := 0
    for scanner.Scan() {
        text := scanner.Text()
        chanStr <- text
        counter++
    }
    close(chanStr)
    log.Info("Finished reading ", counter, " rows")
}(FILEPATH, c1)
```

Next, we define `Worker` outside `main()` function. What this function does is continously receiving string data from a channel, parse it to `type User`, and insert it to BQ Table. When the channel is closed, it will stop.

```go
func deployWorker(ch <-chan string, project, dataset, table string, wg *sync.WaitGroup) {
    // Create BQ client and context
    client, ctx := getBQClient(project)
    // Looping to receive data from channel
    for {
        strJSON, more := <-ch
        if !more {
            break
        } else {
            user := parseUserFromJSONStr(strJSON)
            users := []*User{&user}
            // Insert to BQ table
            bqErr := insertUsersToBQTable(ctx, client, dataset, table, users)
            if bqErr == nil {
                log.Info(fmt.Sprintf("Inserted %d rows - %s", len(users), strJSON))
            }
        }
    }
    client.Close()
    wg.Done()
}
```

Run it as a goroutine.

```go
var wg sync.WaitGroup
wg.Add(1)
go deployWorker(c1, PROJECT, DATASET, TABLE, &wg)
wg.Wait()
log.Info("Done in ", time.Since(start).Seconds(), "seconds")
```

Don't forget that goroutines are asynchronous, the program will done immediately. That's why we use `WaitGroup`, it enable goroutine to be done first and then proceed. It is one of very important Go native features, `WaitGroup` usually used for waiting multiple parallel goroutines.

This Go script is avaliable at `___/main.go`. Let's run it using smaller sample.

```bash
➜  go-json-to-bq git:(master) go run main.go --filepath=./students-10.json.txt
INFO[0000] Creating Table "myproject.mydataset.users" 
INFO[0000] [{"name":"id","type":"NUMERIC","mode":"NULLABLE"},{"name":"name","type":"STRING","mode":"NULLABLE"}] 
INFO[0001] Reading file                                 
INFO[0001] Consume rows                                 
INFO[0001] Inserted 1 rows - {"id":0,"name":"ccc"}      
INFO[0002] Inserted 1 rows - {"id":1,"name":"bbb"}      
INFO[0002] Inserted 1 rows - {"id":2,"name":"aaa"}  
...
INFO[0005] Done in 5.308954153seconds
```

It done in five seconds, however duration will propotionally larger with file size, so let's use other approaches.

### Using multiple insertions

Lorem ipsum dolor sit amet, consectetur adipisicing elit. Aliquam rerum, ratione impedit necessitatibus facere soluta odio repellat asperiores neque! Sunt iusto quia suscipit amet inventore eum, vel molestiae reiciendis alias.

### Using multiple workers and insertions

Lorem ipsum dolor sit amet, consectetur adipisicing elit. Aliquam rerum, ratione impedit necessitatibus facere soluta odio repellat asperiores neque! Sunt iusto quia suscipit amet inventore eum, vel molestiae reiciendis alias.

### Benchmark

Lorem ipsum dolor sit amet, consectetur adipisicing elit. Aliquam rerum, ratione impedit necessitatibus facere soluta odio repellat asperiores neque! Sunt iusto quia suscipit amet inventore eum, vel molestiae reiciendis alias.

