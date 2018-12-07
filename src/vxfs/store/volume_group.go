package store

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
import . "vxfs/dao/store"

const (
	MaxVolumeSize = 8 * 1024 * 1024 * 1024
)

type VolumeGroup struct {
	DataDir     string
	IndexDir    string
	dataFreeMB  uint64
	indexFreeMB uint64
	counters    *StoreCounters

	current *VolumeFile
	rwlock  sync.RWMutex
	volumes map[int64]*VolumeFile

	stats  *StoreStats
	ticker *libs.VxTicker

	keyCache   *KeyCache
	vidMaker   *libs.SnowFlake
	dataPlock  *libs.ProcessLock
	indexPlock *libs.ProcessLock
}

func NewVolumeGroup(dataDir string, indexDir string, dataFreeMB int, indexFreeMB int, statsRefresh int) (g *VolumeGroup, err error) {
	if err = libs.TestWriteDir(dataDir); err != nil {
		glog.Errorf("testWriteDir(\"%s\") error(%v)", dataDir, err)
		return
	}
	if err = libs.TestWriteDir(indexDir); err != nil {
		glog.Errorf("testWriteDir(\"%s\") error(%v)", indexDir, err)
		return
	}

	g = &VolumeGroup{}
	g.DataDir = dataDir
	g.IndexDir = indexDir
	g.dataFreeMB = uint64(dataFreeMB)
	g.indexFreeMB = uint64(indexFreeMB)
	g.counters = &StoreCounters{}
	g.volumes = make(map[int64]*VolumeFile)
	g.stats = &StoreStats{}
	g.ticker = libs.NewVxTicker(g.refreshStats, time.Duration(statsRefresh)*time.Second)
	g.keyCache = NewKeyCache()
	g.vidMaker, _ = libs.NewSnowFlake(int64(1 + libs.Rand.Intn(libs.MaxMachineId)))
	g.dataPlock = libs.NewProcessLock(dataDir+"/", "store data")
	g.indexPlock = libs.NewProcessLock(indexDir+"/", "store index")

	if err = g.init(); err != nil {
		g.Close()
		g = nil
		return
	}

	g.ticker.Tick()
	g.ticker.Start()
	return
}

func (g *VolumeGroup) init() (err error) {
	if err = g.dataPlock.Lock(); err != nil {
		glog.Errorf("VolumeGroup: \"%s\" data lock error(%v)", g.DataDir, err)
		return err
	}
	if err = g.indexPlock.Lock(); err != nil {
		glog.Errorf("VolumeGroup: \"%s\" index lock error(%v)", g.IndexDir, err)
		return err
	}
	files, err := ioutil.ReadDir(g.DataDir)
	if err != nil {
		return
	}
	for _, file := range files {
		name := file.Name()
		if m, _ := regexp.MatchString("^vdata-[0-9]+$", name); m {
			vid, _ := strconv.ParseInt(name[6:], 10, 64)

			vdFile := filepath.Join(g.DataDir, fmt.Sprintf("vdata-%d", vid))
			viFile := filepath.Join(g.IndexDir, fmt.Sprintf("vindex-%d", vid))

			var v *VolumeFile
			if v, err = NewVolumeFile(vid, g.keyCache, vdFile, viFile); err != nil {
				glog.Errorf("VolumeGroup: \"%s\" \"%d\" init file error(%v)", g.DataDir, vid, err)
				return
			}
			g.volumes[vid] = v
			g.counters.FileCount += 1
		}
	}
	for _, v := range g.volumes {
		if v.Data.Size < MaxVolumeSize && (g.current == nil || v.Data.Size < g.current.Data.Size) {
			g.current = v
		}
	}
	return
}

func (g *VolumeGroup) allocVolume() (v *VolumeFile, err error) {
	g.rwlock.Lock()
	defer g.rwlock.Unlock()

	if g.current != nil && g.current.Data.Size < MaxVolumeSize {
		v = g.current
		return
	}

	vid, _ := g.vidMaker.NextId()
	vdFile := filepath.Join(g.DataDir, fmt.Sprintf("vdata-%d", vid))
	viFile := filepath.Join(g.IndexDir, fmt.Sprintf("vindex-%d", vid))
	if v, err = NewVolumeFile(vid, g.keyCache, vdFile, viFile); err != nil {
		return
	}
	g.volumes[vid] = v
	g.current = v
	g.counters.FileCount += 1
	return
}

func (g *VolumeGroup) Read(req *ReadRequest, res *ReadResponse) (err error) {
	var (
		k *KeyBlock
		v *VolumeFile
	)
	if k = g.keyCache.Get(req.Key); k == nil {
		err = ErrStoreNotExists
		return
	}
	g.rwlock.RLock()
	v, _ = g.volumes[k.Vid]
	g.rwlock.RUnlock()
	if err = v.Read(k, res); err != nil {
		return
	}
	atomic.AddUint64(&g.counters.ReadCount, uint64(1))
	atomic.AddUint64(&g.counters.ReadBytes, uint64(k.Size))
	return
}

func (g *VolumeGroup) Write(req *WriteRequest, res *WriteResponse) (err error) {
	if g.stats.DataFreeMB < g.dataFreeMB {
		err = ErrDataNoSpace
		return
	}
	if g.stats.IndexFreeMB < g.indexFreeMB {
		err = ErrIndexNoSpace
		return
	}

	var (
		k *KeyBlock
		v *VolumeFile
	)
	if k = g.keyCache.Get(req.Key); k != nil {
		err = ErrStoreExists
		return
	}
	if v, err = g.allocVolume(); err != nil {
		glog.Errorf("VolumeGroup: \"%s\" \"%d\" allocVolume() error(%v)", g.DataDir, err)
		return
	}
	if k, err = v.Write(req); err != nil {
		return
	}
	atomic.AddUint64(&g.counters.WriteCount, uint64(1))
	atomic.AddUint64(&g.counters.WriteBytes, uint64(k.Size))
	return
}

func (g *VolumeGroup) Delete(req *DeleteRequest, res *DeleteResponse) (err error) {
	var (
		k *KeyBlock
		v *VolumeFile
	)
	if k = g.keyCache.Get(req.Key); k == nil {
		return
	}

	g.rwlock.RLock()
	v, _ = g.volumes[k.Vid]
	g.rwlock.RUnlock()

	if err = v.Delete(k); err != nil {
		return
	}
	g.keyCache.Del(req.Key)
	return
}

func (g *VolumeGroup) refreshStats() {
	g.stats.DataFreeMB, _ = libs.GetDiskFreeSpace(g.DataDir, 2)
	g.stats.IndexFreeMB, _ = libs.GetDiskFreeSpace(g.IndexDir, 2)
	g.stats.Counters = *g.counters

	g.counters.ReadCount = 0
	g.counters.ReadBytes = 0
	g.counters.WriteCount = 0
	g.counters.WriteBytes = 0
	g.counters.DeleteCount = 0
}

func (g *VolumeGroup) Stats(req *StatsRequest, res *StatsResponse) (err error) {
	res.Stats = *g.stats
	return
}

func (g *VolumeGroup) Close() {
	g.rwlock.Lock()
	defer g.rwlock.Unlock()

	g.ticker.Stop()
	for _, v := range g.volumes {
		v.Close()
	}
	g.dataPlock.Unlock()
	g.indexPlock.Unlock()
}
