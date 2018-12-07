package store

type WriteRequest struct {
	Key  int64
	Meta []byte
	Data []byte
}

type WriteResponse struct {
}

type ReadRequest struct {
	Key int64
}

type ReadResponse struct {
	Meta []byte
	Data []byte
	Size int32
}

type DeleteRequest struct {
	Key int64
}

type DeleteResponse struct {
}

type StatsRequest struct {
}

type StatsResponse struct {
	Stats StoreStats
}
