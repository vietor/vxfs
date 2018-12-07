package libs

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// snowflake bits
// -----------------
// | timestamp     | --- 42 bits
// | machine       | --- 10 bits
// | sequence      | --- 12 bits
// -----------------

const (
	maxMachineId    = -1 ^ (-1 << 10)
	maxSequenceMask = -1 ^ (-1 << 12)
)

func twTimestamp() int64 {
	return time.Now().UnixNano()/int64(time.Millisecond) - int64(1288834974657)
}

func waitTwTimestamp(current int64) int64 {
	timestamp := twTimestamp()
	for timestamp <= current {
		timestamp = twTimestamp()
	}
	return timestamp
}

type SnowFlake struct {
	machineId     int64
	sequence      int64
	lastTimestamp int64
	safeLock      sync.Mutex
}

func NewSnowFlake(machineId int64) (*SnowFlake, error) {
	if machineId < 0 || machineId >= maxMachineId {
		return nil, errors.New(fmt.Sprintf("MachineId: %d error, limit [0,%d)", machineId, maxMachineId))
	}
	i := &SnowFlake{}
	i.machineId = machineId
	return i, nil
}

func (i *SnowFlake) UnsafeNextId() (uint64, error) {
	timestamp := twTimestamp()
	if timestamp < i.lastTimestamp {
		return 0, errors.New(fmt.Sprintf("Clock moved backwards. Refusing %d milliseconds", i.lastTimestamp-timestamp))
	}
	if i.lastTimestamp == timestamp {
		i.sequence = (i.sequence + 1) & maxSequenceMask
		if i.sequence == 0 {
			timestamp = waitTwTimestamp(timestamp)
		}
	} else {
		i.sequence = 0
	}
	i.lastTimestamp = timestamp
	return uint64((timestamp << 22) | (i.machineId << 12) | i.sequence), nil
}

func (i *SnowFlake) NextId() (uint64, error) {
	i.safeLock.Lock()
	defer i.safeLock.Unlock()
	return i.UnsafeNextId()
}

func (i *SnowFlake) NextIds(count int) ([]uint64, error) {
	if count < 1 || count > maxSequenceMask {
		return nil, errors.New(fmt.Sprintf("NextIds count: %d error, limit to [1,%d)", count, maxSequenceMask))
	}

	i.safeLock.Lock()
	defer i.safeLock.Unlock()

	var (
		err error
		ids = make([]uint64, count)
	)
	for n := 0; n < count; n++ {
		if ids[n], err = i.UnsafeNextId(); err != nil {
			return nil, err
		}
	}
	return ids, nil
}
