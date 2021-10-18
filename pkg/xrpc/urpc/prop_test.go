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

	"g.tesamc.com/IT/zaipkg/xtest"

	"github.com/elastic/go-hdrhistogram"
	"github.com/templexxx/tsc"
)

// === RUN   Test_Lat_UDS
// ping-pong with: 128 bytes min: 4736, avg: 6414.33, max: 48383, iops: 155656.26
// percentiles (nsec):
// |  1.00th=[4863],  5.00th=[4927], 10.00th=[4927], 20.00th=[4927],
// | 30.00th=[4991], 40.00th=[5055], 50.00th=[5119], 60.00th=[5183],
// | 70.00th=[5183], 80.00th=[6719], 90.00th=[9023], 95.00th=[15999],
// | 99.00th=[18687], 99.50th=[18687], 99.90th=[22207], 99.95th=[27455],
// | 99.99th=[36479]
// --- PASS: Test_Lat_UDS (0.74s)
func Test_Lat_UDS(t *testing.T) {

	if !xtest.IsPropEnabled() {
		t.Skip("skip prop testing")
	}

	testLatency(true, getRandomAddr(), "", 128, 100000)
	defer cleanSockets()
}

// === RUN   Test_Lat_TCP
// ping-pong with: 128 bytes min: 7808, avg: 10862.77, max: 115711, iops: 91921.57
// percentiles (nsec):
// |  1.00th=[7935],  5.00th=[7935], 10.00th=[7935], 20.00th=[8191],
// | 30.00th=[8255], 40.00th=[8319], 50.00th=[8319], 60.00th=[8447],
// | 70.00th=[8511], 80.00th=[11391], 90.00th=[21119], 95.00th=[26367],
// | 99.00th=[30719], 99.50th=[31039], 99.90th=[39935], 99.95th=[43519],
// | 99.99th=[62015]
// --- PASS: Test_Lat_TCP (1.19s)
func Test_Lat_TCP(t *testing.T) {

	if !xtest.IsPropEnabled() {
		t.Skip("skip prop testing")
	}

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

	for n := 0; n < numPing; n++ {
		nread, err := conn.Read(buf)
		if err != nil {
			log.Fatal(err)
		}
		if nread != msgBytes {
			log.Fatalf("bad nread = %d", nread)
		}
		nwrite, err := conn.Write(buf)
		if err != nil {
			log.Fatal(err)
		}
		if nwrite != msgBytes {
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

// Single thread request.
func TestClient_Set_Latency_Single(t *testing.T) {

	if !xtest.IsPropEnabled() {
		t.Skip("skip prop testing")
	}

	addr := getRandomAddr()
	defer cleanSockets()

	s := NewServer(addr, nopHandler())

	if err := s.Start(); err != nil {
		t.Fatalf("cannot start server: %s", err)
	}
	defer s.Stop(nil)

	n := 100000

	c := newTestClient(addr)

	value := make([]byte, 128)
	rand.Read(value)
	key := make([]byte, 8)
	rand.Read(key)

	lat := hdrhistogram.New(100, time.Second.Nanoseconds(), 3)

	err := c.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop(nil)

	jobStart := tsc.UnixNano()
	for i := 0; i < n; i++ {
		start := tsc.UnixNano()
		err := c.Set(1, key, value)
		if err != nil {
			t.Fatal(err)
		}
		_ = lat.RecordValue(tsc.UnixNano() - start)
	}
	cost := tsc.UnixNano() - jobStart

	printLat("set", lat, cost)
}

// Multi threads request.
// Result: https://g.tesamc.com/IT/zmatrix/issues/2#issuecomment-1589
func TestClient_Set_Latency_Concurrency(t *testing.T) {

	if !xtest.IsPropEnabled() {
		t.Skip("skip prop testing")
	}

	addr := getRandomAddr()
	defer cleanSockets()

	s := NewServer(addr, nopHandler())

	if err := s.Start(); err != nil {
		t.Fatalf("cannot start server: %s", err)
	}
	defer s.Stop(nil)

	threads := 64

	c := newTestClient(addr)

	value := make([]byte, 128)
	rand.Read(value)
	key := make([]byte, 8)
	rand.Read(key)

	lat := hdrhistogram.New(100, time.Second.Nanoseconds(), 3)

	n := 100000
	wg := new(sync.WaitGroup)
	wg.Add(threads)

	err := c.Start()
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop(nil)

	jobStart := tsc.UnixNano()
	for j := 0; j < threads; j++ {
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				start := tsc.UnixNano()
				err := c.Set(1, key, value)
				if err != nil {
					t.Error(err)
					return
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
