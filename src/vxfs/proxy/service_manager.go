package proxy

import (
	"math/rand"
	"sync"
	"time"
	"vxfs/dao/name"
	"vxfs/dao/store"
	"vxfs/libs"
	"vxfs/libs/glog"
)

type NameService struct {
	stime  int64
	stats  name.NameStats
	client *libs.RpcClient
}

type StoreService struct {
	id     int32
	stime  int64
	stats  store.StoreStats
	client *libs.RpcClient
}

type ServiceManager struct {
	rand   *rand.Rand
	ticker *libs.VxTicker

	nameDataFreeMB   uint64
	storeDataFreeMB  uint64
	storeIndexFreeMB uint64

	nameLock      sync.RWMutex
	nameService   *NameService
	storeLock     sync.RWMutex
	storeServices map[int32]*StoreService
}

func NewServiceManager(nameDataFreeMB int, storeDataFreeMB int, storeIndexFreeMB int, statsRefresh int) (s *ServiceManager) {
	s = &ServiceManager{}
	s.rand = rand.New(rand.NewSource(time.Now().Unix()))
	s.ticker = libs.NewVxTicker(s.refreshStats, time.Duration(statsRefresh)*time.Second)
	s.nameDataFreeMB = uint64(nameDataFreeMB)
	s.storeDataFreeMB = uint64(storeDataFreeMB)
	s.storeIndexFreeMB = uint64(storeIndexFreeMB)
	s.storeServices = make(map[int32]*StoreService)
	return
}

func (s *ServiceManager) SetNameService(address string) (err error) {
	s.nameLock.Lock()
	defer s.nameLock.Unlock()

	if s.nameService != nil {
		err = ErrInvalidatePrameter
		return
	}
	nameService := &NameService{}
	nameService.client = libs.NetRpcClient(address)
	s.nameService = nameService
	return
}

func (s *ServiceManager) AddStoreService(id int32, address string) (err error) {
	s.storeLock.Lock()
	defer s.storeLock.Unlock()

	if _, ok := s.storeServices[id]; ok {
		err = ErrInvalidatePrameter
		return
	}
	storeService := &StoreService{}
	storeService.id = id
	storeService.client = libs.NetRpcClient(address)
	s.storeServices[id] = storeService
	return
}

func (s *ServiceManager) Startup() {
	s.ticker.Tick()
	s.ticker.Start()
}

func (s *ServiceManager) refreshStats() {
	var (
		err  error
		nreq = &name.StatsRequest{}
		nres = &name.StatsResponse{}
		sreq = &store.StatsRequest{}
		sres = &store.StatsResponse{}
	)
	if err = s.nameService.client.Call("NameService.Stats", nreq, nres); err != nil {
		glog.Warningf("NameService Stats error(%v)\n", err)
	} else {
		s.nameService.stats = nres.Stats
		s.nameService.stime = time.Now().Unix()
	}
	for id, storeService := range s.storeServices {
		if err = storeService.client.Call("StoreService.Stats", sreq, sres); err != nil {
			glog.Warningf("StoreService(%d) Stats error(%v)\n", id, err)
		} else {
			storeService.stats = sres.Stats
			storeService.stime = time.Now().Unix()
		}
	}
}

func (s *ServiceManager) getNameClient() (client *libs.RpcClient, err error) {
	s.nameLock.RLock()
	defer s.nameLock.RUnlock()

	if s.nameService == nil {
		err = ErrNameServiceNoLive
		return
	}
	if s.nameService.stime > 0 && s.nameService.stats.DataFreeMB < s.nameDataFreeMB {
		err = ErrNameServiceNoSpace
		return
	}
	client = s.nameService.client
	return
}

func (s *ServiceManager) getStoreClient(sid int32) (client *libs.RpcClient, err error) {
	s.storeLock.RLock()
	defer s.storeLock.RUnlock()

	var (
		ok           bool
		storeService *StoreService
	)
	if storeService, ok = s.storeServices[sid]; !ok {
		err = ErrStoreServiceNoLive
		return
	}
	client = storeService.client
	return
}

func (s *ServiceManager) GetSid(size int64) (sid int32, err error) {
	count := 0
	frees := make([]*StoreService, len(s.storeServices))

	s.storeLock.RLock()
	for _, v := range s.storeServices {
		if v.stime == 0 || (v.stats.DataFreeMB > s.storeDataFreeMB && v.stats.IndexFreeMB > s.storeIndexFreeMB) {
			frees[count] = v
			count += 1
		}
	}
	s.storeLock.RUnlock()

	if count < 0 {
		err = ErrStoreServiceNoSpace
		return
	}
	sid = frees[s.rand.Intn(count)].id
	return
}

func (s *ServiceManager) ReadName(req *name.ReadRequest, res *name.ReadResponse) (err error) {
	var client *libs.RpcClient
	if client, err = s.getNameClient(); err != nil {
		return
	}
	return client.Call("NameService.Read", req, res)
}

func (s *ServiceManager) WriteName(req *name.WriteRequest, res *name.WriteResponse) (err error) {
	var client *libs.RpcClient
	if client, err = s.getNameClient(); err != nil {
		return
	}
	return client.Call("NameService.Write", req, res)
}

func (s *ServiceManager) DeleteName(req *name.DeleteRequest, res *name.DeleteResponse) (err error) {
	var client *libs.RpcClient
	if client, err = s.getNameClient(); err != nil {
		return
	}
	return client.Call("NameService.Delete", req, res)
}

func (s *ServiceManager) ReadStore(sid int32, req *store.ReadRequest, res *store.ReadResponse) (err error) {
	var client *libs.RpcClient
	if client, err = s.getStoreClient(sid); err != nil {
		return
	}
	return client.Call("StoreService.Read", req, res)
}

func (s *ServiceManager) WriteStore(sid int32, req *store.WriteRequest, res *store.WriteResponse) (err error) {
	var client *libs.RpcClient
	if client, err = s.getStoreClient(sid); err != nil {
		return
	}
	return client.Call("StoreService.Write", req, res)
}

func (s *ServiceManager) DeleteStore(sid int32, req *store.DeleteRequest, res *store.DeleteResponse) (err error) {
	var client *libs.RpcClient
	if client, err = s.getStoreClient(sid); err != nil {
		return
	}
	return client.Call("StoreService.Delete", req, res)
}

func (s *ServiceManager) Cleanup() {
	s.ticker.Stop()

	s.nameLock.Lock()
	if s.nameService != nil {
		s.nameService.client.Close()
		s.nameService = nil
	}
	s.nameLock.Unlock()

	s.storeLock.Lock()
	if s.storeServices != nil {
		for _, v := range s.storeServices {
			v.client.Close()
		}
		s.storeServices = nil
	}
	s.storeLock.Unlock()
}
