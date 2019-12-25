package snowflake

import (
	"errors"
	"math/rand"
	"sync"
	"time"
)

const (
	epoch int64 = 1575023561000
	timeBits = 41
	dataCenterIdBits uint8 = 5
	workerIdBit uint8 = 5
	sequenceBits uint8 = 12
	maskSequence = uint16(1<<sequenceBits - 1)
)

type snowflake struct {
	mu sync.Mutex
	elapsedTime int64
	dataCenterId uint8
	workId uint8
	sequence uint16
}

func (sf *snowflake) NextId() (uint64, error) {
	sf.mu.Lock()
	defer sf.mu.Unlock()

	current := currentElapsedTime()
	if sf.elapsedTime > current {
		return 0, errors.New("Clock moved backwards. Refusing to generate id for " + string(sf.elapsedTime - current) + " milliseconds")
	}

	if sf.elapsedTime == current {
		sf.sequence = sf.nextSequence() & maskSequence
		if sf.sequence == 0 {
			time.Sleep(time.Microsecond * 1)
			sf.elapsedTime = currentElapsedTime()
		}
	} else {
		sf.elapsedTime = current
		sf.sequence = 0
		sf.sequence = sf.nextSequence()
	}
	return sf.toID()
}

func (sf *snowflake) toID() (uint64, error) {
	return uint64(sf.elapsedTime) << (dataCenterIdBits + workerIdBit + sequenceBits) |
		uint64(sf.dataCenterId) << (workerIdBit + sequenceBits) |
		uint64(sf.workId) << sequenceBits |
		uint64(sf.sequence), nil
}

func (sf *snowflake) nextSequence() uint16  {
	if sf.sequence == 0 {
		return uint16(rand.Int31n(10))
	}

	return sf.sequence + 1
}

func NewSnowflake(dataCenterId uint8, workId uint8) *snowflake {
	sf := snowflake{dataCenterId: dataCenterId, workId: workId}
	return &sf
}

func currentElapsedTime() int64 {
	microTime := time.Now().UnixNano()/1e6
	return microTime - epoch
}

func Decompose(uid uint64) map[string]uint64 {
	sequence := uint64(1 << sequenceBits - 1) & uid
	workerId := (uid >> sequenceBits) & uint64(1 << workerIdBit - 1)
	dataCenterId := (uid >> (sequenceBits + workerIdBit)) & uint64(1 << dataCenterIdBits - 1)
	elapsedTime := uid >> (sequenceBits + workerIdBit + dataCenterIdBits)
	timestamp := elapsedTime + uint64(epoch)

	return map[string]uint64{
		"timestamp":timestamp,
		"dataCenterId":dataCenterId,
		"workerId":workerId,
		"sequence":sequence,
	}
}
