package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/mroth/weightedrand"
	"google.golang.org/grpc"
)

const minKeys = 10
const maxKeys = 1000000

var keyChooser KeyChooser

type KeyChooser struct {
	*weightedrand.Chooser
	sync.RWMutex
}

type Choice struct {
	Prefix []byte `json:"prefix"`
	Weight uint   `json:"weight"`
}

type HammerFile struct {
	Choices []Choice `json:"choices"`
}

func (kc *KeyChooser) Replace(c *weightedrand.Chooser) {
	kc.Lock()
	defer kc.Unlock()
	kc.Chooser = c
}

func (kc *KeyChooser) Load(path string) {
	f, err := os.ReadFile(path)
	if err != nil {
		exit(err)
	}

	var config HammerFile
	err = json.Unmarshal(f, &config)
	if err != nil {
		exit(err)
	}

	choices := []weightedrand.Choice{}
	for _, c := range config.Choices {
		choices = append(choices, weightedrand.NewChoice(c.Prefix, c.Weight))
	}

	chooser, err := weightedrand.NewChooser(choices...)
	if err != nil {
		exit(err)
	}

	kc.Replace(chooser)
}

func (kc *KeyChooser) Key() []byte {
	kc.RLock()
	defer kc.RUnlock()

	// what
	prefix := kc.Pick().([]byte)
	suffix := randomKey(8 - len(prefix))
	return bytes.Join([][]byte{prefix, suffix}, []byte{})
}

func init() {
	rand.Seed(time.Now().UnixNano())
	keyChooser = KeyChooser{}
}

func main() {
	faddrs := flag.String("addr", "localhost:8000", "addresses to hammer (comma-separated)")
	fworkers := flag.Int("workers", 100, "number of workers to run in parallel")
	finterval := flag.Int("interval", 100, "max time to sleep between rpcs (ms)")
	fcount := flag.Int("count", 0, "number of requests to send before terminating (default: no limit)")
	ffile := flag.String("keys-file", "", "path to config")
	flag.Parse()

	// Replace default logger.
	logger := log.New(os.Stdout, "", 0)
	*log.Default() = *logger

	ctx, cancel := context.WithCancel(context.Background())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sig
		cancel()
	}()

	wg := sync.WaitGroup{}

	if *ffile != "" {
		keyChooser.Load(*ffile)

		ticker := time.NewTicker(1000 * time.Second)
		done := make(chan bool)

		go func() {
			for {
				select {
				case <-done:
					return
				case <-ticker.C:
					keyChooser.Load(*ffile)
				}
			}
		}()
	} else {
		chooser, err := weightedrand.NewChooser(weightedrand.NewChoice("", 1))
		if err != nil {
			exit(err)
		}

		keyChooser.Replace(chooser)
	}

	addrs := strings.Split(*faddrs, ",")
	for n := 0; n < *fworkers; n++ {

		// If a request limit was set, divide it up equally-ish between the
		// workers. Otherwise leave it as MaxInt.
		c := math.MaxInt
		if *fcount > 0 {
			c = *fcount / *fworkers

			// add remainder to the last worker
			if n == *fworkers-1 {
				c += *fcount % *fworkers
			}
		}

		wg.Add(1)
		go runWorker(ctx, &wg, addrs[n%len(addrs)], *finterval, c)
	}

	// Block until workers self-terminate (because they hit the request limit)
	// or SIGINT is received and workers are cancelled.
	wg.Wait()
}

func newClient(ctx context.Context, addr string) pbkv.KVClient {
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
	if err != nil {
		exit(err)
	}

	return pbkv.NewKVClient(conn)
}

func runWorker(ctx context.Context, wg *sync.WaitGroup, addr string, interval int, count int) {
	c := newClient(ctx, addr)
	cnt := 0

	// Keys which have been PUT
	keys := [][]byte{}
	vals := [][]byte{}

	for {

		// Probably cancellation
		if ctx.Err() != nil {
			break
		}

		// Stop once this worker has sent enough requests. (Note: when -count is
		// lower than -workers, some will have zero requests, so we might return
		// before sending any.)
		if cnt >= count {
			break
		}

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
		cnt += 1
	}

	wg.Done()
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

	log.Printf("GET: %s -- %s", req.Key, status)
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

	log.Printf("PUT: %s -- %s", req.Key, res)
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
