package main

import (
	"fmt"
	"sync"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher, mutex *sync.Mutex, cache *map[string]bool, ch chan int) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.

	defer func() { ch <- 1 }() // can do this instead of ch <- 1 for each return
	if depth <= 0 {
		//ch <- 1
		return
	}

	mutex.Lock()
	if (*cache)[url] {
		mutex.Unlock()
		//ch <- 1
		return
	}
	(*cache)[url] = true
	mutex.Unlock()

	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		//ch <- 1
		return
	}
	fmt.Printf("found: %s %q\n", url, body)

	channels := []chan int{}
	for _, u := range urls {
		chAux := make(chan int)
		channels = append(channels, chAux)
		go Crawl(u, depth-1, fetcher, mutex, cache, chAux)
	}

	// wait for child processes to finish
	for _, ch := range channels {
		<-ch
	}
	//ch <- 1
	return
}

func main() {
	ch := make(chan int, 1)
	Crawl("https://golang.org/", 4, fetcher, &mutex, &cache, ch)
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// cache that stores the fetched urls
var cache = map[string]bool{}
var mutex sync.Mutex

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}
