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

	for {
		// TODO: PUT
		getOnce(ctx, c)
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
}

func getOnce(ctx context.Context, client pbkv.KVClient) {
	req := &pbkv.GetRequest{
		Key: randomKey(8),
	}

	res := "OK"

	_, err := client.Get(ctx, req)
	if err != nil {
		res = fmt.Sprintf("Error: %s", err)
	}

	fmt.Printf("Get: %s -- %s\n", req.Key, res)
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
