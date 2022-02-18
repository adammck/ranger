package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/lthibault/jitterbug"
	"google.golang.org/grpc"
)

type ConfigQPS struct {
	Create uint `json:"create"`
	Read   uint `json:"read"`
	Update uint `json:"update"`
	Delete uint `json:"delete"`
}

type ConfigWorker struct {
	Prefix string    `json:"prefix"`
	QPS    ConfigQPS `json:"qps"`
}

type Config struct {
	Workers []ConfigWorker `json:"workers"`
}

func Load(path string) Config {
	f, err := os.ReadFile(path)
	if err != nil {
		exit(fmt.Errorf("os.ReadFile: %v", err))
	}

	var c Config
	err = json.Unmarshal(f, &c)
	if err != nil {
		exit(fmt.Errorf("json.Unmarshal: %v", err))
	}

	return c
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	faddrs := flag.String("addr", "localhost:8000", "addresses to hammer (comma-separated)")
	fconfig := flag.String("config", "", "path to config")
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

	if *fconfig == "" {
		exit(fmt.Errorf("required: -config"))
	}

	config := Load(*fconfig)

	// Set up pool of clients, one per address
	addrs := strings.Split(*faddrs, ",")
	clients := make([]pbkv.KVClient, len(addrs))
	for i := range addrs {
		clients[i] = newClient(ctx, addrs[i])
	}

	for _, w := range config.Workers {
		RunGroup(ctx, clients, &wg, w)
	}

	wg.Wait()
}

func newClient(ctx context.Context, addr string) pbkv.KVClient {
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure())
	if err != nil {
		exit(err)
	}

	return pbkv.NewKVClient(conn)
}

type Group struct {
	config  ConfigWorker
	clients []pbkv.KVClient

	keys [][]byte
	vals [][]byte
	mu   sync.RWMutex
}

func RunGroup(ctx context.Context, clients []pbkv.KVClient, wg *sync.WaitGroup, w ConfigWorker) {
	g := Group{
		config:  w,
		clients: clients,
		keys:    [][]byte{},
		vals:    [][]byte{},
	}

	wg.Add(4)

	// TODO: Dispatch the RPCs into goroutine pools rather than on the ticker
	//       thread. There is no way we can reach significant qps right now.

	// Create
	go g.run(ctx, wg, g.config.QPS.Create, func() {
		k := g.RandomKey() // With prefix
		v := randomLetters(1 + rand.Intn(255))

		// Outside the lock.
		ok := putOnce(ctx, g.client(), k, v)
		if ok {
			g.mu.Lock()
			g.keys = append(g.keys, k)
			g.vals = append(g.vals, v)
			g.mu.Unlock()
		}
	})

	// Read
	go g.run(ctx, wg, g.config.QPS.Read, func() {
		g.mu.RLock()
		l := len(g.keys)
		if l == 0 {
			g.mu.RUnlock()
			return
		}
		i := rand.Intn(l)
		k := g.keys[i]
		v := g.vals[i]
		g.mu.RUnlock()

		// Outside the lock.
		getOnce(ctx, g.client(), k, v)
	})

	// Update
	go g.run(ctx, wg, g.config.QPS.Update, func() {
		g.mu.RLock()
		l := len(g.keys)
		if l == 0 {
			g.mu.RUnlock()
			return
		}
		i := rand.Intn(l)
		k := g.keys[i]
		v := randomLetters(1 + rand.Intn(255))
		g.mu.RUnlock()

		// Note that we release the lock while sending the request, but we don't
		// worry about update because this is the only thread *updating* vals.

		ok := putOnce(ctx, g.client(), k, v)
		if ok {
			g.mu.Lock()
			g.vals[i] = v
			g.mu.Unlock()
		}
	})

	// Delete
	go g.run(ctx, wg, g.config.QPS.Delete, func() {
		// Not implemented
	})
}

func (g *Group) run(ctx context.Context, wg *sync.WaitGroup, qps uint, f func()) {
	if qps == 0 {
		wg.Done()
		return
	}

	nsInterval := int(1*time.Second) / int(qps)
	d := time.Duration(nsInterval)

	// Sleep randomly up to the interval to stagger workers with same QPS.
	//time.Sleep(time.Duration(rand.Intn(nsInterval)))

	// Jitter by 10%
	ticker := jitterbug.New(d, &jitterbug.Norm{Stdev: d / 10})

	for {
		select {
		case <-ctx.Done():
			goto exit // looooool
		case <-ticker.C:
			f()
		}
	}

exit:
	wg.Done()
}

// client returns a random client to send a request via.
func (g *Group) client() pbkv.KVClient {
	return g.clients[rand.Intn(len(g.clients))]
}

func (g *Group) RandomKey() []byte {
	prefix := []byte(g.config.Prefix)
	suffix := randomLetters(8 - len(prefix))
	return bytes.Join([][]byte{prefix, suffix}, []byte{})
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

func randomLetters(n int) []byte {
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
