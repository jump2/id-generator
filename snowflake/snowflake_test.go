package snowflake

import (
	"fmt"
	"github.com/deckarep/golang-set"
	"math/rand"
	"runtime"
	"testing"
	"time"
)

var sf *snowflake
var dataCenterId uint8
var workerId uint8

func init()  {
	rand.Seed(time.Now().UnixNano())
	dataCenterId = uint8(rand.Int31n(1 << dataCenterIdBits))
	workerId = uint8(rand.Int31n(1 << workerIdBit))
	sf = NewSnowflake(dataCenterId, workerId)
}

func nextId(t *testing.T) uint64 {
	id, err := sf.NextId()
	if err != nil {
		t.Errorf(err.Error())
	}

	return id
}

func TestSnowflake_NextId(t *testing.T) {
	currentTime := uint64(time.Now().UnixNano() / 1e6)
	id := nextId(t)
	parts := Decompose(id)

	actualTimestamp := parts["timestamp"]
	if actualTimestamp < currentTime || actualTimestamp > currentTime + 1{
		t.Errorf("unexpected time: %d", actualTimestamp)
	}

	actualDataCenterId := parts["dataCenterId"]
	if actualDataCenterId != uint64(dataCenterId) {
		t.Errorf("unexpected data center id: %d", actualDataCenterId)
	}

	actualWorkerId := parts["workerId"]
	if actualWorkerId != uint64(workerId) {
		t.Errorf("unexpected work id: %d", actualWorkerId)
	}

	actualSequence := parts["sequence"]
	if actualSequence > (1 << sequenceBits - 1) {
		t.Errorf("unexpected sequence: %d", actualSequence)
	}

	fmt.Println("snowflake id:", id)
	fmt.Println("decompose:", parts)
}

func TestSnowflakeParallel(t *testing.T) {
	numCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(numCPU)
	fmt.Println("number of cpu:", numCPU)

	consumer := make(chan uint64)

	const numID = 10000
	generate := func() {
		for i := 0; i < numID; i++ {
			consumer <- nextId(t)
		}
	}

	const numGenerator = 10
	for i := 0; i < numGenerator; i++ {
		go generate()
	}

	set := mapset.NewSet()
	for i := 0; i < numID*numGenerator; i++ {
		id := <-consumer
		if set.Contains(id) {
			t.Fatal("duplicated id")
		} else {
			set.Add(id)
		}
	}
	fmt.Println("number of id:", set.Cardinality())
}

func BenchmarkGenerateId(b *testing.B) {
	b.StopTimer()
	sf := NewSnowflake(1, 1)

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		sf.NextId()
	}
}
