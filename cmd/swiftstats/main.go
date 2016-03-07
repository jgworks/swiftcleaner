package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/ncw/swift"
)

var (
	chanLimit       = flag.Int("limit", 20, "Limit of number of concurent connections")
	containerPrefix = flag.String("container", "sample", "Container prefix")
	objectPrefix    = flag.String("object", "", "Object prefix")
	swiftUsername   = flag.String("swift-username", "${ST_USER}", "Swift Username")
	swiftApiKey     = flag.String("swift-apikey", "${ST_KEY}", "Swift Username")
	swiftAuthUrl    = flag.String("swift-authurl", "${ST_AUTH}", "Swift Auth URL")
	swiftRetries    = flag.Int("swift-retries", 3, "Pumber of times to retry")
	printFlag       = flag.Bool("print", true, "Print each object")
)

func main() {
	flag.Parse()

	// expand environment variables
	*swiftUsername = os.ExpandEnv(*swiftUsername)
	*swiftApiKey = os.ExpandEnv(*swiftApiKey)
	*swiftAuthUrl = os.ExpandEnv(*swiftAuthUrl)

	var lock sync.WaitGroup
	var printlock sync.WaitGroup
	ch := make(chan struct{}, *chanLimit)
	outch := make(chan string)

	//ctx, cancel := context.WithCancel(context.Background())

	printlock.Add(1)
	go func() {
		defer func() {
			printlock.Done()
		}()
		for out := range outch {
			fmt.Println(out)
		}
	}()

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
		lock.Add(1)
		go func() {
			defer func() {
				<-ch
				lock.Done()
			}()

			obj, _, err := swiftConnection.Object(*containerPrefix+line[:3], line)
			if err != nil {
				log.Printf("%s: %s", err, line)
				return
			}

			outch <- fmt.Sprintf("%s %d", obj.Name, obj.Bytes)
		}()
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	lock.Wait()
	close(outch)
	printlock.Wait()
}
