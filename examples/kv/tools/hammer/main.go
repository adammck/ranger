package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"google.golang.org/grpc"
)

const minKeys = 10
const maxKeys = 1000000

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	faddrs := flag.String("addr", "localhost:8000", "addresses to hammer (comma-separated)")
	fworkers := flag.Int("workers", 100, "number of workers to run in parallel")
	flag.Parse()

	ctx := context.Background()

	sig := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sig, syscall.SIGINT)

	go func() {
		s := <-sig
		fmt.Fprintf(os.Stderr, "Signal: %v\n", s)
		done <- true
	}()

	addrs := strings.Split(*faddrs, ",")
	for n := 0; n < *fworkers; n++ {
		go runWorker(ctx, addrs[n%len(addrs)])
	}

	<-done
}

func newClient(ctx context.Context, addr string) pbkv.KVClient {
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
	if err != nil {
		exit(err)
	}

	return pbkv.NewKVClient(conn)
}

func runWorker(ctx context.Context, addr string) {
	c := newClient(ctx, addr)

	// Keys which have been PUT
	keys := [][]byte{}

	for {
		n := rand.Intn(10)

		// always PUT until minKeys reached, then 10% of reqs until maxKeys.
		if len(keys) < minKeys || (n == 0 && len(keys) < maxKeys) {
			k := randomKey(8)
			ok := putOnce(ctx, c, k)
			if ok {
				keys = append(keys, k)
			}

		} else {
			// GET a random key which we have put.
			getOnce(ctx, c, keys[rand.Intn(len(keys))])
		}

		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
}

func getOnce(ctx context.Context, client pbkv.KVClient, key []byte) {
	req := &pbkv.GetRequest{
		Key: key,
	}

	res := "OK"

	_, err := client.Get(ctx, req)
	if err != nil {
		res = fmt.Sprintf("Error: %s", err)
	}

	fmt.Printf("GET: %s -- %s\n", req.Key, res)
}

func putOnce(ctx context.Context, client pbkv.KVClient, key []byte) bool {
	req := &pbkv.PutRequest{
		Key:   key,
		Value: randomKey(rand.Intn(256)),
	}

	res := "OK"
	ok := true

	_, err := client.Put(ctx, req)
	if err != nil {
		res = fmt.Sprintf("Error: %s", err)
		ok = false
	}

	fmt.Printf("PUT: %s -- %s\n", req.Key, res)
	return ok
}

func randomKey(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return b
}

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func exit(err error) {
	fmt.Fprintf(os.Stderr, "Error: %s\n", err)
	os.Exit(1)
}
