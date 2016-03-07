package main

import (
	"bufio"
	"flag"
	"log"
	"math"
	"os"
	"sync"

	"golang.org/x/net/context"

	"github.com/ncw/swift"
)

var (
	maxSizeMb       = flag.Float64("size", 80, "Maxium object size in MiB")
	chanLimit       = flag.Int("limit", 20, "Limit of number of concurent connections")
	containerPrefix = flag.String("container", "sample", "Container prefix")
	objectPrefix    = flag.String("object", "", "Object prefix")
	swiftUsername   = flag.String("swift-username", "${ST_USER}", "Swift Username")
	swiftApiKey     = flag.String("swift-apikey", "${ST_KEY}", "Swift Username")
	swiftAuthUrl    = flag.String("swift-authurl", "${ST_AUTH}", "Swift Auth URL")
	swiftRetries    = flag.Int("swift-retries", 3, "Number of times to retry")
	printFlag       = flag.Bool("print", true, "Print each object")

	fMaxBytes int64
	ch        chan struct{}
	byteSumCh chan int64
	lock      *sync.WaitGroup
)

func main() {
	flag.Parse()

	// expand environment variables
	*swiftUsername = os.ExpandEnv(*swiftUsername)
	*swiftApiKey = os.ExpandEnv(*swiftApiKey)
	*swiftAuthUrl = os.ExpandEnv(*swiftAuthUrl)

	fMaxBytes = int64(*maxSizeMb * math.Pow(2, 20))
	ch = make(chan struct{}, *chanLimit)
	byteSumCh = make(chan int64)
	//totalCh := make(chan int64)
	lock = new(sync.WaitGroup)

	// ctx, cancel := context.WithCancel(context.Background())

	// // signal channel to catch ^C and USR1
	// c := make(chan os.Signal, 3)
	// signal.Notify(c, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)
	// go func() {
	// 	defer func() {
	// 		signal.Stop(c)
	// 	}()
	// 	for sig := range c {
	// 		switch sig {
	// 		case syscall.SIGINT, syscall.SIGTERM:
	// 			cancel()
	// 		case syscall.SIGUSR1:
	// 			panic("Signal USR1 Received")
	// 		}
	// 	}
	// }()

	// create connection to swift
	swiftConnection := &swift.Connection{
		UserName: *swiftUsername,
		ApiKey:   *swiftApiKey,
		AuthUrl:  *swiftAuthUrl,
		Retries:  *swiftRetries,
	}
	defer func() {
		swiftConnection.UnAuthenticate()
	}()

	// log.Println("Max Bytes:", int64(fMaxBytes))

	// // create a channel to receive byte counts on and sum the results
	// go func(ctx context.Context, byteSumCh chan int64, totalCh chan int64) {
	// 	var totalSize int64
	// 	for {
	// 		select {
	// 		case <-ctx.Done():
	// 			totalCh <- totalSize
	// 			return
	// 		case size := <-byteSumCh:
	// 			totalSize += size
	// 		}
	// 	}
	// }(ctx, byteSumCh, totalCh)

	err := swiftConnection.Authenticate()
	if err != nil {
		log.Fatal(err)
	}

	file, err := os.Open("2016-01.md5")
	if err != nil {
		log.Fatal(err)

	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()

		ch <- struct{}{}
		go func() {
			defer func() {
				<-ch
			}()
			obj, _, err := swiftConnection.Object("sample"+line[:3], line)
			if err != nil {
				log.Printf("%s: %s", err, line)
				return
			}

			log.Println(obj.Name, obj.Bytes)
		}()
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// // wait for all findObjects calls to complete
	// lock.Wait()
	// // send signal to stop accepting bytes to total and stop any other goroutines
	// cancel()
	// total := <-totalCh
	// log.Printf("Total size: %d (%0.2f GiB)", total, float64(total)/math.Pow(2, 30))
}

func findContainers(ctx context.Context, swiftConnection *swift.Connection) error {
	containers, err := swiftConnection.ContainersAll(&swift.ContainersOpts{Prefix: *containerPrefix})
	if err != nil {
		return err
	}

	// create a goroutine for each container up to the size of channel ch
loop:
	for _, c := range containers {
		var c = c

		select {
		// if the context is done we quit
		case <-ctx.Done():
			break loop
		// attempt to take a processing slot and increment the WaitGroup
		case ch <- struct{}{}:
			lock.Add(1)

			go func() {
				defer func() {
					// We are done so decrement the WaitGroup and put back
					// a processing slot
					lock.Done()
					<-ch
				}()
				err := findObjects(ctx, swiftConnection, c.Name)
				if err != nil {
					// only print the error if the context isn't done
					select {
					case <-ctx.Done():
						return
					default:
						log.Println(err)
					}
				}
			}()
		}
	}

	return nil
}

func findObjects(ctx context.Context, swiftConnection *swift.Connection, container string) error {
	err := swiftConnection.ObjectsWalk(container, &swift.ObjectsOpts{Prefix: *objectPrefix}, func(opts *swift.ObjectsOpts) (interface{}, error) {
		objs, err := swiftConnection.Objects(container, opts)
		if err != nil {
			return nil, err
		}

		for _, o := range objs {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
				if o.Bytes > fMaxBytes {
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case byteSumCh <- o.Bytes:
						if *printFlag {
							log.Printf("%s/%s: %d", container, o.Name, o.Bytes)
						}
					}
				}
			}
		}

		return objs, nil
	})
	if err != nil {
		return err
	}
	return nil
}
