---
layout: single
classes: wide
title:  "Inserting 1 Million data into BigQuery Table"
cover: "https://images.unsplash.com/photo-1552474458-59a25d46b57f?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=1951&q=80"
description: Inserting 1 Million data into BigQuery Table.
date:   2020-03-01 12:51:00 +0700
categories:
  - blog
tags:
  - golang
  - bigquery
# toc: true
excerpt: "Exploring Go language and it's features to write high-performance program."
header:
  overlay_image: "https://images.unsplash.com/photo-1549113640-ac1757c2d2b3?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=2050&q=80"
  overlay_filter: 0.5 # same as adding an opacity of 0.5 to a black background
  caption: "Photo credit: [**Unsplash**](https://unsplash.com/@eli_allan_photography)"
#   teaser: "https://images.unsplash.com/photo-1549113640-ac1757c2d2b3?ixlib=rb-1.2.1&ixid=eyJhcHBfaWQiOjEyMDd9&auto=format&fit=crop&w=2050&q=80"
  # actions:
  #   - label: "More Info"
  #     url: "https://unsplash.com"
share: False
related: False
comments: True
---

Moving data from multiple sources into multiple databases is one of the data engineer's responsibilities. It is always recommended to use simple and direct approach first, especially when the data is small enough, like iterating data one by one and inserting it at the same time. However this approach will not going to work well on larger sized data or higher throughput stream data.

In this blog post, we will demonstrate how to insert JSON text file into Google's BigQuery Table using Go language and it's various native libraries (channel, goroutine, waitgroup). Go is known for one of the best language to write high-performance programs due to it's native libraries that make concurrent and parallel programming easier.

## Preparation

For preparation, let's create a text file containing 1 million rows of single JSON per row, using a python script provided below <sup>[[1]]({{site.base_url}}#cite_note-1)</sup>.

```sh
# Generate dataset
python3 gen-txt.py
# you will see files like this
# -rw-rw-r-- 1 pandu pandu  220 Mar  5 18:31 students-10.json.txt
# -rw-rw-r-- 1 pandu pandu 2.3K Mar  5 18:31 students-100.json.txt
# -rw-rw-r-- 1 pandu pandu  24K Mar  5 18:31 students-1000.json.txt
# -rw-rw-r-- 1 pandu pandu 244K Mar  5 18:31 students-10000.json.txt
# -rw-rw-r-- 1 pandu pandu 2.5M Mar  5 18:31 students-100000.json.txt
# -rw-rw-r-- 1 pandu pandu  26M Mar  5 18:31 students-1000000.json.txt
```

We will get a text files like this. In this case, each row contain 22 characters = 22 Bytes.

```json
{"id":1,"name":"aaa"}
{"id":2,"name":"bbb"}
...
```

For BigQuery, make sure we have GCP key JSON file that has access to modify BigQuery table and don't forget to export it to `GOOGLE_APPLICATION_CREDENTIALS` env var. After that, we all set.

## Part One: Simple approach

We will do three approaches to do insertion. But first, let's use simplest approach. Read text file one line by one and insert it to a BigQuery table one at a time.

<figure>
	<a href="../../assets/images/i1m-part-one.png">
    <img src="../../assets/images/i1m-part-one.png">
  </a>
	<!-- <figcaption><a href="http://www.flickr.com/photos/80901381@N04/7758832526/" title="Morning Fog Emerging From Trees by A Guy Taking Pictures, on Flickr">Morning Fog Emerging From Trees by A Guy Taking Pictures, on Flickr</a>.</figcaption> -->
</figure>

In this diagram, there are three main components: 1) `File Reader`, 2) Channel `c1`, and 3) `Worker`. `File Reader` will read text file one by one line and send it to channel `c1`. Channel `c1` to share string data. `Worker` is responsible for receiving string data from channel `c1` and inserting it into a BigQuery table. 

Channel is a native type in Go that enable us to send/receive values and share it accross coroutine (goroutine). Let's define Channel `c1` first.

```go
c1 = make(chan string)
```

Next, we create `File Reader` as an anonymous function and immediately run it as a goroutine. Using `go` keyword in front of function call can enable it to run asynchrounously/non-blocking to main function.

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

Next, we define `Worker` outside main function. What this function does is to continously receiving string data from a channel, parse it, and insert it to BQ Table. When the channel is closed, it will stop.

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

Don't forget that goroutines are asynchronous, the program will done be immediately. That's why we use `WaitGroup`, it enables goroutine to be done first and then proceed. It is one of very important Go native features, `WaitGroup` usually used for waiting multiple parallel goroutines.

This Go script is avaliable at `cmd/main1/main1.go` <sup>[[1]]({{site.base_url}}#cite_note-1)</sup>.

<!-- ```bash
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

It done in five seconds, however duration will propotionally larger with file size, so let's use other approaches. -->

## Part Two: Multiple Rows Insertions

According to BigQuery Streaming Insert Documentation <sup>[[2]]({{site.base_url}}#cite_note-2)</sup>., we can insert multiple rows in one API call. Let's modify our program.

<figure>
	<a href="../../assets/images/i1m-part-two.png">
    <img src="../../assets/images/i1m-part-two.png">
  </a>
	<!-- <figcaption><a href="http://www.flickr.com/photos/80901381@N04/7758832526/" title="Morning Fog Emerging From Trees by A Guy Taking Pictures, on Flickr">Morning Fog Emerging From Trees by A Guy Taking Pictures, on Flickr</a>.</figcaption> -->
</figure>

In this new architecture, we add two more components. First is `Buffer` and second is shannel `c2`. `Buffer` receive string data from `c1`, collect it temporary into an array with length `n`, and then send it to `c2` simultaneously. Let's run it as a goroutine.

```go
// Put string data to a buffer and send it to another channel
go func(chInput <-chan string, chOutput chan<- []string, n int) {
    rows := []string{}
    counter := 0
    for {
        row, more := <-chInput
        if !more {
            break
        }
        rows = append(rows, row)
        counter++
        if counter == n {
            chOutput <- rows
            rows = []string{}
            counter = 0
        }
    }
    close(chOutput)
}(c1, c2, BUFFER_LENGTH)
```

Channel `c2` is a `[]string` type channel. It only receive/send an array of string.

```go
c1 = make(chan string)
c2 = make(chan []string)
```

Next, edit `Worker` to receive a new type of channel `c2`.

```go
func deployWorker(ch <-chan []string, project, dataset, table string, wg *sync.WaitGroup) {
    // Create BQ client and context
    client, ctx := getBQClient(project)
    // Looping to receive data from channel
    for {
        strJSONs, more := <-ch
        if !more {
            break
        } else {
            // Parser also modified
            users := []*User{}
            for _, strJSON := range strJSONs {
                user := parseUserFromJSONStr(strJSON)
                users = append(users, &user)
            }
            // Insert to BQ table
            bqErr := insertUsersToBQTable(ctx, client, dataset, table, users)
            if bqErr == nil {
                log.Info(fmt.Sprintf("Inserted %d rows - %s", len(users), strJSONs))
            }
        }
    }
    client.Close()
    wg.Done()
}
```

Finally the second approach is done. Using right number of `n`, it can improve insertion speed significantly. This Go script is avaliable at `cmd/main2/main2.go` <sup>[[1]]({{site.base_url}}#cite_note-1)</sup>.

## Part Three: Multiple Workers and Multiple Rows Insertion

For the third and final approach, we will take advantage of Go's goroutine. Go can spawn multiple goroutines in large number and significantly faster than other coroutine in other programming languages. For this approach, we will deploy multiple workers.

<figure>
	<a href="../../assets/images/i1m-part-three.png">
    <img src="../../assets/images/i1m-part-three.png">
  </a>
	<!-- <figcaption><a href="http://www.flickr.com/photos/80901381@N04/7758832526/" title="Morning Fog Emerging From Trees by A Guy Taking Pictures, on Flickr">Morning Fog Emerging From Trees by A Guy Taking Pictures, on Flickr</a>.</figcaption> -->
</figure>

In this third approach, we will simply deploy more workers using for loop. Let's modify the main function a bit.

```go
var wg sync.WaitGroup
for idx := 0; idx < WORKER_NUMBER; idx++ {
    wg.Add(1)
    go deployWorker(c2, PROJECT, DATASET, TABLE, &wg)
}
wg.Wait()
log.Info("Done in ", time.Since(start).Seconds(), "seconds")
```

This Go script is avaliable at `cmd/main3/main3.go` <sup>[[1]]({{site.base_url}}#cite_note-1)</sup>.

## Benchmark

For deciding max number of buffer and workers, we need to read BigQuery Streaming API quotas and limits <sup>[[3]]({{site.base_url}}#cite_note-3)</sup>.

```
Maximum rows per request: 10,000 rows per request, maximum of 500 rows is recommended
Concurrent API requests, per user: 300
API requests per second, per user — 100
```

Benchmark is taken using same type of machine, `n1-standard-1 (1 vCPU, 3.75 GB mem)`<sup>[[4]]({{site.base_url}}#cite_note-4)</sup>. Using multiple JSON text files generated at different sizes, we benchmark three approaches and measure time taken.

| File     | Parameter   | 1.000 rows | 10.000    | 100.000   | 1.000.000 |
|:---------|:------------|-----------:|----------:|----------:|----------:|
| main.go  | w1 - n1     |   312.164s | 3242.766s |       n/a |       n/a |
| main2.go | w1 - n100   |     4.599s |   35.735s |  381.251s | 3738.669s |
| main3.go | w4 - n100   |     1.453s |    9.137s |   95.666s |  939.175s |
| main3.go | w16 - n100  |     0.808s |    2.938s |   24.754s |  224.630s |
| main3.go | w64 - n100  |     0.848s |    1.643s |   11.667s |   62.624s |
| main3.go | w300 - n500 |     0.934s |    1.296s |    4.081s |   14.787s |

As we can see, higher number of `w` and `n` make insertion faster. Using our highest configuration enable us to insert one million rows in a mere 14 seconds!

## Conclusion

In this blog post we learned to use concurrency/parallelism concept to make our programs do the job faster. We also explored Go language and it's native libraries (channel, goroutines, waitgroup) to develop a high-performance program.

One thing to remember is that everything came with a tradeoff. We also need to watch out for memory and/or API calls limit when using large number of goroutines.

Also checkout my open source project [ps2bq](https://github.com/anpandu/ps2bq){:target="_blank"}. It is a tool to insert GCP PubSub messages into BigQuery table. It was also developed using Go and concurrency/parallelism concept.

## Reference

* <a id="cite_note-1">[1]</a> [i1m github](https://github.com/anpandu/i1m){:target="_blank"}
* <a id="cite_note-2">[2]</a> [BigQuery Streaming Insert Documentation](https://cloud.google.com/bigquery/streaming-data-into-bigquery){:target="_blank"}
* <a id="cite_note-3">[3]</a> [BigQuery Quota Documentation](https://cloud.google.com/bigquery/quotas#streaming_inserts){:target="_blank"}
* <a id="cite_note-4">[4]</a> [VM instances pricing](https://cloud.google.com/compute/vm-instance-pricing#n1_standard_machine_types){:target="_blank"}

