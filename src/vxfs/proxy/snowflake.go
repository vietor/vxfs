package proxy

import (
	"errors"
	"fmt"
	"sync"
	"time"
	"vxfs/libs/glog"
)

// snowflake bits
// -----------------
// | timestamp     | --- 42 bits
// | datacenter    | --- 5 bits
// | machine       | --- 5 bits
// | sequence      | --- 12 bits
// -----------------

const (
	maxDatacenterId = -1 ^ (-1 << 5)
	maxMachineId    = -1 ^ (-1 << 5)
	sequenceMask    = -1 ^ (-1 << 12)
	MinSnowFlakeId  = 225901025343 << 22 // 2018-01-01
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
	maxCount      int
	datacenterId  int64
	machineId     int64
	sequence      int64
	lastTimestamp int64
	safeLock      sync.Mutex
}

func NewSnowFlake(datacenterId int64, machineId int64, maxCount int) (*SnowFlake, error) {
	if datacenterId < 0 || datacenterId > maxDatacenterId {
		return nil, errors.New(fmt.Sprintf("datacenterId: %d error, 0-%d", datacenterId, maxDatacenterId))
	}
	if machineId < 0 || machineId > maxMachineId {
		return nil, errors.New(fmt.Sprintf("machineId: %d error, 0-%d", machineId, maxMachineId))
	}
	if maxCount == 0 {
		maxCount = sequenceMask - 1
	} else if maxCount < 1 || maxCount >= sequenceMask {
		return nil, errors.New(fmt.Sprintf("maxCount: %d error, must > 0 & < %d", maxCount, sequenceMask))
	}
	i := &SnowFlake{}
	i.datacenterId = datacenterId
	i.machineId = machineId
	i.maxCount = maxCount
	return i, nil
}

func (i *SnowFlake) UnsafeNextId() (uint64, error) {
	timestamp := twTimestamp()
	if timestamp < i.lastTimestamp {
		glog.Errorf("Clock is moving backwards.  Rejecting requests until %d.", i.lastTimestamp)
		return 0, errors.New(fmt.Sprintf("Clock moved backwards. Refusing %d milliseconds", i.lastTimestamp-timestamp))
	}
	if i.lastTimestamp == timestamp {
		i.sequence = (i.sequence + 1) & sequenceMask
		if i.sequence == 0 {
			timestamp = waitTwTimestamp(timestamp)
		}
	} else {
		i.sequence = 0
	}
	i.lastTimestamp = timestamp
	return uint64((timestamp << 22) | (i.datacenterId << 17) | (i.machineId << 12) | i.sequence), nil
}

func (i *SnowFlake) NextId() (uint64, error) {
	i.safeLock.Lock()
	defer i.safeLock.Unlock()
	return i.UnsafeNextId()
}

func (i *SnowFlake) NextIds(count int) ([]uint64, error) {
	if count < 1 || count > i.maxCount {
		return nil, errors.New(fmt.Sprintf("NextIds count: %d error, limit to 1-%d", count, i.maxCount))
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
