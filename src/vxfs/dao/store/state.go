package store

type StoreCounters struct {
	FileCount   int32  `json:"file_count"`
	ReadCount   uint64 `json:"read_count"`
	ReadBytes   uint64 `json:"read_bytes"`
	WriteCount  uint64 `json:"write_count"`
	WriteBytes  uint64 `json:"write_bytes"`
	DeleteCount uint64 `json:"delete_count"`
}

type StoreStats struct {
	DataFreeMB  uint64        `json:"data_freemb"`
	IndexFreeMB uint64        `json:"index_freemb"`
	Counters    StoreCounters `json:"counters"`
}
