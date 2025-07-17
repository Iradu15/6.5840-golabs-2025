package main

/*
	Modify the Crawl function to fetch URLs in parallel without fetching the same URL twice.
	Hint: you can keep a cache of the URLs that have been fetched on a map, but maps alone are not safe for concurrent use!
*/

import (
	"fmt"
	"sync"
	"time"
)

func contains(url string, cache []string) bool {
	for _, u := range cache {
		if u == url {
			return true
		}
	}
	return false
}

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}

// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func Crawl(url string, depth int, fetcher Fetcher, mutex *sync.Mutex, cache *[]string) {
	// TODO: Fetch URLs in parallel.
	// TODO: Don't fetch the same URL twice.
	// This implementation doesn't do either:
	if depth <= 0 {
		return
	}
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("found: %s %q\n", url, body)
	mutex.Lock()
	*cache = append(*cache, url)
	fmt.Printf("Added %v to cache: %v\n", url, cache)

	for _, u := range urls {

		ans := contains(u, *cache)
		if !ans {
			go Crawl(u, depth-1, fetcher, mutex, cache)
		}
	}
	mutex.Unlock()
	time.Sleep(10 * time.Second)
	return
}

func main() {
	Crawl("https://golang.org/", 4, fetcher, &mutex, &cache)
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
var cache = []string{}
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
