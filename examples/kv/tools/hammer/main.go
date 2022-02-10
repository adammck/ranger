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
	finterval := flag.Int("interval", 100, "max time to sleep between rpcs (ms)")
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
		go runWorker(ctx, addrs[n%len(addrs)], *finterval)
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

func runWorker(ctx context.Context, addr string, interval int) {
	c := newClient(ctx, addr)

	// Keys which have been PUT
	keys := [][]byte{}
	vals := [][]byte{}

	for {
		n := rand.Intn(10)

		// always PUT new values until minKeys reached, then 10% until maxKeys.
		if len(keys) < minKeys || (n == 0 && len(keys) < maxKeys) {
			k := randomKey(8)
			v := randomKey(1 + rand.Intn(255))
			ok := putOnce(ctx, c, k, v)
			if ok {
				keys = append(keys, k)
				vals = append(vals, v)
			}

		} else if n == 0 {
			// Replace values 10% of the time after maxKeys
			i := rand.Intn(len(keys))
			k := keys[i]
			v := randomKey(1 + rand.Intn(255))
			ok := putOnce(ctx, c, k, v)
			if ok {
				vals[i] = v
			}

		} else {
			// GET a random key which we have put.
			i := rand.Intn(len(keys))
			getOnce(ctx, c, keys[i], vals[i])
		}

		time.Sleep(time.Duration(rand.Intn(interval)) * time.Millisecond)
	}
}

func getOnce(ctx context.Context, client pbkv.KVClient, key []byte, val []byte) {
	req := &pbkv.GetRequest{
		Key: key,
	}

	status := "OK"

	res, err := client.Get(ctx, req)
	if err != nil {
		status = fmt.Sprintf("Error: %s", err)
	} else if string(res.Value) != string(val) {
		status = fmt.Sprintf("Bad: %q != %q", val, res.Value)
	}

	fmt.Printf("GET: %s -- %s\n", req.Key, status)
}

func putOnce(ctx context.Context, client pbkv.KVClient, key []byte, val []byte) bool {
	req := &pbkv.PutRequest{
		Key:   key,
		Value: val,
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
