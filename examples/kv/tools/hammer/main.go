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
	"sync/atomic"
	"syscall"
	"time"

	pbkv "github.com/adammck/ranger/examples/kv/proto/gen"
	"github.com/lthibault/jitterbug"
	"google.golang.org/grpc"
)

type Stats struct {
	creates uint64
	reads   uint64
	updates uint64
	deletes uint64
}

func (s *Stats) Total() int {
	return int(s.creates + s.reads + s.updates + s.deletes)
}

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

	t := time.Now()
	stats := Stats{}

	for _, w := range config.Workers {
		RunGroup(ctx, clients, &stats, &wg, w)
	}

	wg.Wait()

	runTime := time.Since(t)

	fmt.Printf("Ran for %s\n", runTime)
	fmt.Printf("- Creates: %d (%d/s)\n", stats.creates, int(float64(stats.creates)/runTime.Seconds()))
	fmt.Printf("- Reads: %d (%d/s)\n", stats.reads, int(float64(stats.reads)/runTime.Seconds()))
	fmt.Printf("- Updates: %d (%d/s)\n", stats.updates, int(float64(stats.updates)/runTime.Seconds()))
	fmt.Printf("- Deletes: %d (%d/s)\n", stats.deletes, int(float64(stats.deletes)/runTime.Seconds()))
	fmt.Printf("- Total: %d (%d/s)\n", stats.Total(), int(float64(stats.Total())/runTime.Seconds()))
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

	// Must be the same number of both.
	keys [][]byte
	vals []Value

	// Only take this lock when adding new key+val pairs.
	mu sync.RWMutex
}

type Value struct {
	value  []byte
	locker uint32
}

func RunGroup(ctx context.Context, clients []pbkv.KVClient, stats *Stats, wg *sync.WaitGroup, w ConfigWorker) {
	g := Group{
		config:  w,
		clients: clients,
		keys:    [][]byte{},
		vals:    []Value{},
	}

	wg.Add(4)

	// Create
	go g.run(ctx, wg, g.config.QPS.Create, func() {
		k := g.RandomKey() // With prefix
		v := randomLetters(1 + rand.Intn(255))

		// Outside the lock.
		ok := putOnce(ctx, g.client(), k, v)
		if ok {
			g.mu.Lock()
			g.keys = append(g.keys, k)
			g.vals = append(g.vals, Value{value: v})
			g.mu.Unlock()
		}

		atomic.AddUint64(&stats.creates, 1)
	})

	// Read
	go g.run(ctx, wg, g.config.QPS.Read, func() {
		g.mu.RLock()
		l := len(g.keys)
		g.mu.RUnlock()

		// Skip if no values have been written yet.
		if l == 0 {
			return
		}

		i := rand.Intn(l)

		// Try to lock the value while we get it, so no update can change it
		// from under us. (This is likely to happen when the number of values is
		// small and the query rate is high.) Skip if we can't get the lock.
		if !atomic.CompareAndSwapUint32(&g.vals[i].locker, 0, 1) {
			return
		}

		getOnce(ctx, g.client(), g.keys[i], g.vals[i].value)

		atomic.StoreUint32(&g.vals[i].locker, 0)
		atomic.AddUint64(&stats.reads, 1)
	})

	// Update
	go g.run(ctx, wg, g.config.QPS.Update, func() {
		g.mu.RLock()
		l := len(g.keys)
		g.mu.RUnlock()

		// Skip if no values have been written yet.
		if l == 0 {
			return
		}

		i := rand.Intn(l)

		if !atomic.CompareAndSwapUint32(&g.vals[i].locker, 0, 1) {
			return
		}

		v := randomLetters(1 + rand.Intn(255))
		ok := putOnce(ctx, g.client(), g.keys[i], v)
		if ok {
			g.vals[i].value = v
		}

		atomic.StoreUint32(&g.vals[i].locker, 0)
		atomic.AddUint64(&stats.updates, 1)
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
			wg.Add(1)
			go func() {
				f()
				wg.Done()
			}()
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
	ok := true

	res, err := client.Get(ctx, req)
	if err != nil {
		status = fmt.Sprintf("Error: %s", err)
		ok = false
	} else if string(res.Value) != string(val) {
		status = fmt.Sprintf("Bad: %q != %q", val, res.Value)
		ok = false
	}

	if !ok {
		log.Printf("GET: %s -- %s", req.Key, status)
	}
}

func putOnce(ctx context.Context, client pbkv.KVClient, key []byte, val []byte) bool {
	req := &pbkv.PutRequest{
		Key:   key,
		Value: val,
	}

	status := "OK"
	ok := true

	_, err := client.Put(ctx, req)
	if err != nil {
		status = fmt.Sprintf("Error: %s", err)
		ok = false
	}

	if !ok {
		log.Printf("PUT: %s -- %s", req.Key, status)
	}

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
