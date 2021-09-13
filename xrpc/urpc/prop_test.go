package urpc

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/templexxx/tsc"

	"github.com/elastic/go-hdrhistogram"
)

func TestClient_Set_Latency(t *testing.T) {

	// if !xtest.IsPropEnabled() {
	// 	t.Skip("skip prop testing")
	// }

	addr := getRandomAddr()

	s := NewServer(addr, nopHandler())

	if err := s.Start(); err != nil {
		t.Fatalf("cannot start server: %s", err)
	}
	defer s.Stop(nil)

	c := newTestClient(addr)
	c.Conns = 1

	err := c.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop(nil)

	value := make([]byte, 1024)
	rand.Read(value)
	key := make([]byte, 8)
	rand.Read(key)

	lat := hdrhistogram.New(100, time.Second.Nanoseconds(), 3)

	n := 1000000
	jobStart := tsc.UnixNano()
	for i := 0; i < n; i++ {
		start := tsc.UnixNano()
		err := c.Set(key, value)
		if err != nil {
			t.Fatal(err)
		}
		_ = lat.RecordValue(tsc.UnixNano() - start)
	}
	cost := tsc.UnixNano() - jobStart

	printLat("set", lat, cost)
}

func TestClient_Set_Latency_Concurrency(t *testing.T) {

	// if !xtest.IsPropEnabled() {
	// 	t.Skip("skip prop testing")
	// }

	addr := getRandomAddr()

	s := NewServer(addr, nopHandler())

	if err := s.Start(); err != nil {
		t.Fatalf("cannot start server: %s", err)
	}
	defer s.Stop(nil)

	c := newTestClient(addr)
	c.Conns = 4

	err := c.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop(nil)

	value := make([]byte, 1024)
	rand.Read(value)
	key := make([]byte, 8)
	rand.Read(key)

	lat := hdrhistogram.New(100, time.Second.Nanoseconds(), 3)

	n := 1000000
	wg := new(sync.WaitGroup)
	wg.Add(4)

	jobStart := tsc.UnixNano()
	for j := 0; j < 4; j++ {
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				start := tsc.UnixNano()
				err := c.Set(key, value)
				if err != nil {
					t.Fatal(err)
				}
				_ = lat.RecordValueAtomic(tsc.UnixNano() - start)
			}
		}()
	}
	wg.Wait()
	cost := tsc.UnixNano() - jobStart

	printLat("set", lat, cost)
}

func printLat(name string, lats *hdrhistogram.Histogram, cost int64) {
	fmt.Println(fmt.Sprintf("%s min: %d, avg: %.2f, max: %d, iops: %.2f",
		name, lats.Min(), lats.Mean(), lats.Max(), float64(lats.TotalCount())/float64(cost)))
	fmt.Println("percentiles (nsec):")
	fmt.Print(fmt.Sprintf(
		"|  1.00th=[%d],  5.00th=[%d], 10.00th=[%d], 20.00th=[%d],\n"+
			"| 30.00th=[%d], 40.00th=[%d], 50.00th=[%d], 60.00th=[%d],\n"+
			"| 70.00th=[%d], 80.00th=[%d], 90.00th=[%d], 95.00th=[%d],\n"+
			"| 99.00th=[%d], 99.50th=[%d], 99.90th=[%d], 99.95th=[%d],\n"+
			"| 99.99th=[%d]\n",
		lats.ValueAtQuantile(1), lats.ValueAtQuantile(5), lats.ValueAtQuantile(10), lats.ValueAtQuantile(20),
		lats.ValueAtQuantile(30), lats.ValueAtQuantile(40), lats.ValueAtQuantile(50), lats.ValueAtQuantile(60),
		lats.ValueAtQuantile(70), lats.ValueAtQuantile(80), lats.ValueAtQuantile(90), lats.ValueAtQuantile(95),
		lats.ValueAtQuantile(99), lats.ValueAtQuantile(99.5), lats.ValueAtQuantile(99.9), lats.ValueAtQuantile(99.95),
		lats.ValueAtQuantile(99.99)))
}
