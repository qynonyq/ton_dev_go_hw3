package storage

import "time"

type Block struct {
	SeqNo       uint32 `gorm:"primaryKey;autoIncrement:false;"`
	Workchain   int32
	Shard       int64
	ProcessedAt time.Time
}
