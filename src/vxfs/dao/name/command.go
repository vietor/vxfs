package name

type WriteRequest struct {
	Name string
	Sid  int32
	Key  uint64
}

type WriteResponse struct {
}

type ReadRequest struct {
	Name string
}

type ReadResponse struct {
	Sid int32
	Key uint64
}

type DeleteRequest struct {
	Name string
}

type DeleteResponse struct {
}

type StatsRequest struct {
}

type StatsResponse struct {
	Stats NameStats
}
