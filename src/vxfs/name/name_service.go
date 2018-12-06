package name

import . "vxfs/dao/name"

type NameService struct {
	g *NameGroup
}

func NewNameService(g *NameGroup) (s *NameService) {
	s = &NameService{g}
	return
}

func (s *NameService) Write(req *WriteRequest, res *WriteResponse) (err error) {
	return s.g.Write(req, res)
}

func (s *NameService) Read(req *ReadRequest, res *ReadResponse) (err error) {
	return s.g.Read(req, res)
}

func (s *NameService) Delete(req *DeleteRequest, res *DeleteResponse) (err error) {
	return s.g.Delete(req, res)
}

func (s *NameService) Stats(req *StatsRequest, res *StatsResponse) (err error) {
	return s.g.Stats(req, res)
}
