package name

type NameCounters struct {
	FileCount   int32  `json:"file_count"`
	ReadCount   uint64 `json:"read_count"`
	WriteCount  uint64 `json:"write_count"`
	DeleteCount uint64 `json:"delete_count"`
}

type NameStats struct {
	DataFreeMB uint64       `json:"data_freemb"`
	Counters   NameCounters `json:"counters"`
}
