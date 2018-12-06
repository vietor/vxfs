package store

import . "vxfs/dao/store"

type StoreService struct {
	g *VolumeGroup
}

func NewStoreService(g *VolumeGroup) (s *StoreService) {
	s = &StoreService{g}
	return
}

func (s *StoreService) Write(req *WriteRequest, res *WriteResponse) (err error) {
	return s.g.Write(req, res)
}

func (s *StoreService) Read(req *ReadRequest, res *ReadResponse) (err error) {
	return s.g.Read(req, res)
}

func (s *StoreService) Delete(req *DeleteRequest, res *DeleteResponse) (err error) {
	return s.g.Delete(req, res)
}

func (s *StoreService) Stats(req *StatsRequest, res *StatsResponse) (err error) {
	return s.g.Stats(req, res)
}
