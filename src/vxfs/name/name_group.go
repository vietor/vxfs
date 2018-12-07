package name

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"vxfs/libs"
	"vxfs/libs/glog"
)
import . "vxfs/dao/name"

const (
	MaxNameSize = 8 * 1024 * 1024 * 1024
)

type NameGroup struct {
	DataDir    string
	dataFreeMB uint64
	counters   *NameCounters

	current *NameFile
	rwlock  sync.RWMutex
	namefs  map[int64]*NameFile

	stats  *NameStats
	ticker *libs.VxTicker

	nameCache *NameCache
	nidMaker  *libs.SnowFlake
	dataPlock *libs.ProcessLock
}

func NewNameGroup(dataDir string, dataFreeMB int, statsRefresh int) (g *NameGroup, err error) {
	if err = libs.TestWriteDir(dataDir); err != nil {
		glog.Errorf("testWriteDir(\"%s\") error(%v)", dataDir, err)
		return
	}

	g = &NameGroup{}
	g.DataDir = dataDir
	g.dataFreeMB = uint64(dataFreeMB)
	g.counters = &NameCounters{}
	g.namefs = make(map[int64]*NameFile)
	g.stats = &NameStats{}
	g.ticker = libs.NewVxTicker(g.refreshStats, time.Duration(statsRefresh)*time.Second)
	g.nameCache = NewNameCache()
	g.nidMaker, _ = libs.NewSnowFlake(int64(1 + libs.Rand.Intn(libs.MaxMachineId)))
	g.dataPlock = libs.NewProcessLock(dataDir+"/", "name data")

	if err = g.init(); err != nil {
		g.Close()
		g = nil
		return
	}

	g.ticker.Tick()
	g.ticker.Start()
	return
}

func (g *NameGroup) init() (err error) {
	if err = g.dataPlock.Lock(); err != nil {
		glog.Errorf("NameGroup: \"%s\" data lock error(%v)", g.DataDir, err)
		return err
	}
	files, err := ioutil.ReadDir(g.DataDir)
	if err != nil {
		return
	}
	for _, file := range files {
		name := file.Name()
		if m, _ := regexp.MatchString("^ndata-[0-9]+$", name); m {
			var (
				nid int64
				n   *NameFile
			)
			if nid, err = strconv.ParseInt(name[6:], 10, 64); err != nil {
				glog.Errorf("NameGroup: \"%s\" \"%s\" init name error(%v)", g.DataDir, name, err)
				return
			}
			ndFile := filepath.Join(g.DataDir, fmt.Sprintf("ndata-%d", nid))
			if n, err = NewNameFile(nid, g.nameCache, ndFile); err != nil {
				glog.Errorf("NameGroup: \"%s\" \"%d\" init file error(%v)", g.DataDir, nid, err)
				return
			}
			g.namefs[nid] = n
			g.counters.FileCount += 1
		}
	}
	for _, v := range g.namefs {
		if v.Data.Size < MaxNameSize && (g.current == nil || v.Data.Size < g.current.Data.Size) {
			g.current = v
		}
	}
	return
}

func (g *NameGroup) allocName() (n *NameFile, err error) {
	g.rwlock.Lock()
	defer g.rwlock.Unlock()

	if g.current != nil && g.current.Data.Size < MaxNameSize {
		n = g.current
		return
	}

	nid, _ := g.nidMaker.NextId()
	ndFile := filepath.Join(g.DataDir, fmt.Sprintf("ndata-%d", nid))
	if n, err = NewNameFile(nid, g.nameCache, ndFile); err != nil {
		return
	}
	g.namefs[nid] = n
	g.current = n
	g.counters.FileCount += 1
	return
}

func (g *NameGroup) Read(req *ReadRequest, res *ReadResponse) (err error) {
	var (
		k *NameBlock
	)
	if k = g.nameCache.Get(req.Name); k == nil {
		err = ErrNameNotExists
		return
	}

	res.Sid = k.Sid
	res.Key = k.Key
	atomic.AddUint64(&g.counters.ReadCount, uint64(1))
	return
}

func (g *NameGroup) Write(req *WriteRequest, res *WriteResponse) (err error) {
	if g.stats.DataFreeMB < g.dataFreeMB {
		err = ErrDataNoSpace
		return
	}

	var (
		k *NameBlock
		n *NameFile
	)
	if k = g.nameCache.Get(req.Name); k != nil {
		err = ErrNameExists
		return
	}
	if n, err = g.allocName(); err != nil {
		glog.Errorf("NameGroup: \"%s\" \"%d\" allocName() error(%v)", g.DataDir, err)
		return
	}
	if k, err = n.Write(req); err != nil {
		return
	}
	atomic.AddUint64(&g.counters.WriteCount, uint64(1))
	return
}

func (g *NameGroup) Delete(req *DeleteRequest, res *DeleteResponse) (err error) {
	var (
		k *NameBlock
		v *NameFile
	)
	if k = g.nameCache.Get(req.Name); k == nil {
		return
	}

	g.rwlock.RLock()
	v, _ = g.namefs[k.Nid]
	g.rwlock.RUnlock()

	if err = v.Delete(k); err != nil {
		return
	}
	g.nameCache.Del(req.Name)
	atomic.AddUint64(&g.counters.DeleteCount, uint64(1))
	return
}

func (g *NameGroup) refreshStats() {
	g.stats.DataFreeMB, _ = libs.GetDiskFreeSpace(g.DataDir, 2)
	g.stats.Counters = *g.counters

	g.counters.ReadCount = 0
	g.counters.WriteCount = 0
	g.counters.DeleteCount = 0
}

func (g *NameGroup) Stats(req *StatsRequest, res *StatsResponse) (err error) {
	res.Stats = *g.stats
	return
}

func (g *NameGroup) Close() {
	g.rwlock.Lock()
	defer g.rwlock.Unlock()

	g.ticker.Stop()
	for _, v := range g.namefs {
		v.Close()
	}
	g.dataPlock.Unlock()
}
