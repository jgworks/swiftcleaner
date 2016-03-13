package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"reflect"
	"swiftcleaner/bg"
	"sync"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/ncw/swift"

	"gopkg.in/olivere/elastic.v2"
)

//query         = flag.String("query", "score:0 AND source:VTEST AND timestamp:>=%d AND timestamp:<%d", "ES index to search")
var (
	limit         = flag.Int64("limit", 100, "Stop after this many samples")
	statsInterval = flag.Duration("stats-interval", 10*time.Second, "Print stats after this duration")
	esLimit       = flag.Int("limit-es", 1, "Limit on ES requets")
	swiftLimit    = flag.Int("limit-swift", 1, "Limit on Swift requests")
	server        = flag.String("server", "http://esm-001-e.elasticsearch-est.prod.p10.tts.local:9200", "ES index to search")
	index         = flag.String("index", "current_glimpses", "ES index to search")
	query         = flag.String("query", "score:0 AND source:VTEST AND timestamp:<%d", "ES index to search")
	startTimeStr  = flag.String("time-start", "2015-01-01", "Time range start")
	endTimeStr    = flag.String("time-end", "2015-03-13", "Time range end")
	swiftUsername = flag.String("swift-username", "${ST_USER}", "Swift Username")
	swiftApiKey   = flag.String("swift-apikey", "${ST_KEY}", "Swift Username")
	swiftAuthUrl  = flag.String("swift-authurl", "${ST_AUTH}", "Swift Auth URL")
	debug         = flag.Bool("debug", false, "debugging output")
	print         = flag.Bool("print", true, "print normal output")

	esLimiter       bg.Limiter
	swiftLimiter    bg.Limiter
	outch           chan Obj
	swiftConnection *swift.Connection
	stats           *Stats
	_log            *log.Logger
	_debug          *log.Logger
)

type Obj struct {
	Name   string
	Size   int64
	Delete bool
}
type Stats struct {
	mux         sync.RWMutex
	total       int64
	deleteCount int64
	keepCount   int64
	size        uint64
}

func (s *Stats) Total() int64 {
	s.mux.RLock()
	defer s.mux.RUnlock()

	return s.total
}
func (s *Stats) Delete() int64 {
	s.mux.RLock()
	defer s.mux.RUnlock()

	return s.deleteCount
}
func (s *Stats) Keep() int64 {
	s.mux.RLock()
	defer s.mux.RUnlock()

	return s.keepCount
}
func (s *Stats) Size() uint64 {
	s.mux.RLock()
	defer s.mux.RUnlock()

	return s.size
}

func (s *Stats) AddSize(size int64) {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.size += uint64(size)
}

func (s *Stats) IncTotal() {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.total++
}
func (s *Stats) IncDelete() {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.deleteCount++
}
func (s *Stats) IncKeep() {
	s.mux.Lock()
	defer s.mux.Unlock()

	s.keepCount++
}

const dateParse = "2006-01-02"

func main() {
	flag.Parse()
	logFlags := log.LstdFlags
	if *debug {
		logFlags |= log.Lshortfile
		_debug = log.New(os.Stderr, "", logFlags)
	} else {
		_debug = log.New(ioutil.Discard, "", 0)
	}
	_log = log.New(os.Stderr, "", logFlags)

	stats = &Stats{}
	esLimiter = bg.NewLimiter(*esLimit)
	swiftLimiter = bg.NewLimiter(*swiftLimit)
	outch = make(chan Obj)

	startTime, err := time.Parse(dateParse, *startTimeStr)
	if err != nil {
		_log.Fatal(err)
	}
	endTime, err := time.Parse(dateParse, *endTimeStr)
	if err != nil {
		_log.Fatal(err)
	}

	_log.Printf("Searching from %s to %s", startTime.String(), endTime.String())
	_log.Printf("Stats every %q", statsInterval)

	var printlock sync.WaitGroup

	printlock.Add(1)
	go func() {
		defer func() {
			printlock.Done()
		}()
		print_stats := func() {
			_log.Printf("esLimiter.Count: %d swiftLimiter.Count: %d", esLimiter.Count(), swiftLimiter.Count())
			_log.Printf("Total Size: %s Counts: total: %d keep: %d delete: %d", humanize.Bytes(stats.Size()), stats.Total(), stats.Keep(), stats.Delete())
		}
		lastTime := time.Now()
		for obj := range outch {
			if *print {
				_log.Printf("delete: %t %s: %d", obj.Delete, obj.Name, obj.Size)
			}

			stats.AddSize(obj.Size)
			if obj.Delete {
				stats.IncDelete()
			} else {
				stats.IncKeep()
			}

			if time.Since(lastTime) >= *statsInterval {
				print_stats()
				lastTime = time.Now()
			}
		}
		print_stats()
	}()

	// expand environment variables
	*swiftUsername = os.ExpandEnv(*swiftUsername)
	*swiftApiKey = os.ExpandEnv(*swiftApiKey)
	*swiftAuthUrl = os.ExpandEnv(*swiftAuthUrl)

	// create connection to swift
	swiftConnection = &swift.Connection{
		UserName: *swiftUsername,
		ApiKey:   *swiftApiKey,
		AuthUrl:  *swiftAuthUrl,
		Retries:  10,
	}

	err = swiftConnection.Authenticate()
	if err != nil {
		_log.Fatal(err)
	}
	defer func() {
		swiftConnection.UnAuthenticate()
	}()

	c, err := elastic.NewClient(elastic.SetURL(*server))
	if err != nil {
		_log.Fatal("NewClient:", err)
	}

	sampleChan := make(chan Sample)
	go ESService(c, sampleChan)

	queryStr := fmt.Sprintf(*query, endTime.Unix()*1000)
	_log.Printf(`query: "%s"`, queryStr)
	q := elastic.NewQueryStringQuery(queryStr)
	s := c.Scroll(*index).Query(q).Size(100)
esloop:
	for {
		r, err := s.Do()
		if err != nil {
			if err == elastic.EOS {
				break esloop
			} else {
				_log.Fatal("Query:", err)
			}
		} else {
			s.ScrollId(r.ScrollId)
		}

		var sample Sample
		for _, hit := range r.Each(reflect.TypeOf(sample)) {
			if sample, ok := hit.(Sample); ok {
				var sample = sample

				stats.IncTotal()

				sampleChan <- sample

				if *limit > 0 && stats.total >= *limit {
					break esloop
				}
			}
		}
	}

	esLimiter.Wait()
	swiftLimiter.Wait()
	close(outch)
	printlock.Wait()
}

func ESService(c *elastic.Client, samples chan Sample) {
	for sample := range samples {
		var sample = sample

		err := esLimiter.Call(func() {
			_debug.Printf("%s: checking es", sample.Subject)
			delete, err := sample.CanDelete(c)
			if err != nil {
				_log.Println("CanDelete:", err)
			}

			if delete {
				err := swiftLimiter.Call(sample.SwiftSampleSearch)
				if err != nil {
					_log.Println(err)
				}
				err = swiftLimiter.Call(func() { sample.SwiftSandboxSearchWithExt("xml") })
				if err != nil {
					_log.Println(err)
				}
				err = swiftLimiter.Call(func() { sample.SwiftSandboxSearchWithExt("log") })
				if err != nil {
					_log.Println(err)
				}
				err = swiftLimiter.Call(func() { sample.SwiftSandboxSearchWithExt("cab") })
				if err != nil {
					_log.Println(err)
				}
			} else {
				outch <- Obj{
					Name: fmt.Sprintf("%s", sample.Subject),
				}
			}
		})
		if err != nil {
			_log.Println(err)
		}

	}
}

// &{"source":"VTest","subject":"1c465c2ceb53a7595cddc8719eccc7bf","type":"sample","timestamp":1454244141000,"score":0}
type Sample struct {
	Source    string // `json:"source"`
	Subject   string // `json:"subject"`
	Type      string // `json:"type"`
	Timestamp int64  // `json:"timestamp"`
	Score     int64  // `json:"score"`
}

func (sample Sample) CanDelete(c *elastic.Client) (bool, error) {
	q := elastic.NewQueryStringQuery(fmt.Sprintf("subject:%s AND score:>0 AND source:VTEST", sample.Subject))
	s := c.Search(*index).Query(q).From(0).Size(0)

	r, err := s.Do()
	if err != nil {
		return false, err
	}

	return r.Hits.TotalHits == 0, nil
}

func (sample Sample) SwiftSampleSearch() {
	container := "sample" + sample.Subject[:3]

	_debug.Printf("%s/%s: checking swift", container, sample.Subject)

	obj, _, err := swiftConnection.Object(container, sample.Subject)
	if err != nil {
		_debug.Printf("swift error: %s: %s/%s", err, container, sample.Subject)
		return
	}
	_debug.Printf("%s/%s: %d", container, obj.Name, obj.Bytes)
	outch <- Obj{
		Name:   fmt.Sprintf("%s/%s", container, obj.Name),
		Size:   obj.Bytes,
		Delete: true,
	}
	// opts := &swift.ObjectsOpts{
	// 	Prefix: sample.Subject,
	// }
	// objs, err := swiftConnection.Objects(container, opts)
	// if err != nil {
	// 	_log.Printf("swift error: %s: %s", err, sample.Subject)
	// 	return
	// }

	// for _, obj := range objs {
	// 	_log.Printf("%s/%s: %d", container, obj.Name, obj.Bytes)
	// 	outch <- obj.Bytes
	// }
}

//func (sample Sample) SwiftSandboxSearch() {
//	container := "sandbox" + sample.Subject[:3]
//	_log.Printf("%s/%s: checking swift", container, sample.Subject)
//	opts := &swift.ObjectsOpts{
//		Prefix: sample.Subject,
//	}
//	objs, err := swiftConnection.Objects(container, opts)
//	if err != nil {
//		_log.Printf("swift error: %s: %s", err, sample.Subject)
//		return
//	}
//
//	for _, obj := range objs {
//		_log.Printf("%s/%s: %d", container, obj.Name, obj.Bytes)
//		outch <- obj.Bytes
//	}
//}
func (sample Sample) SwiftSandboxSearchWithExt(ext string) {
	container := "sandbox" + sample.Subject[:3]
	objName := fmt.Sprintf("%s.%s", sample.Subject, ext)

	_debug.Printf("%s/%s: checking swift", container, objName)

	obj, _, err := swiftConnection.Object(container, objName)
	if err != nil {
		_debug.Printf("swift error: %s: %s/%s", err, container, objName)
		return
	}

	_debug.Printf("%s/%s: %d", container, obj.Name, obj.Bytes)
	outch <- Obj{
		Name:   fmt.Sprintf("%s/%s", container, obj.Name),
		Size:   obj.Bytes,
		Delete: true,
	}
}
