package urpc

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/templexxx/tsc"

	"github.com/elastic/go-hdrhistogram"
)

func Test_Lat_UDS(t *testing.T) {

	// if !xtest.IsPropEnabled() {
	// 	t.Skip("skip prop testing")
	// }

	testLatency(true, getRandomAddr(), "", 128, 100000)
}

func Test_Lat_TCP(t *testing.T) {

	// if !xtest.IsPropEnabled() {
	// 	t.Skip("skip prop testing")
	// }

	testLatency(false, "", getRandomTCPAddr(), 128, 100000)
}

func domainAndAddress(isUDS bool, unixAddress, tcpAddress string) (string, string) {
	if isUDS {
		return "unix", unixAddress
	} else {
		return "tcp", tcpAddress
	}
}

func server(isUDS bool, unixAddress, tcpAddress string, numPing, msgBytes int) {
	if isUDS {
		if err := os.RemoveAll(unixAddress); err != nil {
			panic(err)
		}
	}

	domain, address := domainAndAddress(isUDS, unixAddress, tcpAddress)
	l, err := net.Listen(domain, address)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	conn, err := l.Accept()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	buf := make([]byte, msgBytes)

	fakeResp := make([]byte, respHeaderSize)

	for n := 0; n < numPing; n++ {
		nread, err := conn.Read(buf)
		if err != nil {
			log.Fatal(err)
		}
		if nread != msgBytes {
			log.Fatalf("bad nread = %d", nread)
		}
		nwrite, err := conn.Write(fakeResp)
		if err != nil {
			log.Fatal(err)
		}
		if nwrite != respHeaderSize {
			log.Fatalf("bad nwrite = %d", nwrite)
		}
	}

	time.Sleep(50 * time.Millisecond)
}

func testLatency(isUDS bool, unixAddress, tcpAddress string, msgBytes, numPings int) {

	go server(isUDS, unixAddress, tcpAddress, numPings, msgBytes)
	time.Sleep(50 * time.Millisecond)

	// This is the client code in the main goroutine.
	domain, address := domainAndAddress(isUDS, unixAddress, tcpAddress)
	conn, err := net.Dial(domain, address)
	if err != nil {
		log.Fatal(err)
	}

	buf := make([]byte, msgBytes)

	lat := hdrhistogram.New(100, time.Second.Nanoseconds(), 3)

	t1 := tsc.UnixNano()
	for n := 0; n < numPings; n++ {

		start := tsc.UnixNano()
		nwrite, err := conn.Write(buf)
		if err != nil {
			log.Fatal(err)
		}
		if nwrite != msgBytes {
			log.Fatalf("bad nwrite = %d", nwrite)
		}
		nread, err := conn.Read(buf)
		if err != nil {
			log.Fatal(err)
		}
		if nread != msgBytes {
			log.Fatalf("bad nread = %d", nread)
		}
		lat.RecordValue(tsc.UnixNano() - start)
	}
	elapsed := tsc.UnixNano() - t1

	printLat(fmt.Sprintf("ping-pong with: %d bytes", msgBytes), lat, elapsed)

	time.Sleep(50 * time.Millisecond)
}

func TestClient_Set_Latency_Single(t *testing.T) {

	// if !xtest.IsPropEnabled() {
	// 	t.Skip("skip prop testing")
	// }

	addr := getRandomAddr()

	// s := NewServer(addr, nopHandler())
	//
	// if err := s.Start(); err != nil {
	// 	t.Fatalf("cannot start server: %s", err)
	// }
	// defer s.Stop(nil)

	n := 100000
	go server(true, addr, "", n, 1024+8+19)
	time.Sleep(50 * time.Millisecond)

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

	n := 100000
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
		name, lats.Min(), lats.Mean(), lats.Max(), float64(lats.TotalCount())/(float64(cost)/float64(time.Second))))
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
