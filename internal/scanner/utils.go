package scanner

import (
	"context"
	"fmt"
	"time"

	"github.com/qynonyq/ton_dev_go_hw3/internal/storage"
	"gorm.io/gorm"

	"github.com/xssnick/tonutils-go/ton"
)

func (s *Scanner) getShardID(shard *ton.BlockIDExt) string {
	return fmt.Sprintf("%d|%d", shard.Workchain, shard.Shard)
}

func (s *Scanner) fillWithNotSeenShards(
	ctx context.Context,
	shards map[string]*ton.BlockIDExt,
	shard *ton.BlockIDExt,
) error {
	// unique key
	key := fmt.Sprintf("%d:%d:%d", shard.Workchain, shard.Shard, shard.SeqNo)
	if _, ok := shards[key]; ok {
		return nil
	}

	shards[key] = shard

	block, err := s.api.GetBlockData(ctx, shard)
	if err != nil {
		return fmt.Errorf("failed to get block data: %w", err)
	}

	parents, err := block.BlockInfo.GetParentBlocks()
	if err != nil {
		return fmt.Errorf("failed to get parent blocks (%d:%d): %w", shard.Workchain, shard.Shard, err)
	}

	for _, parent := range parents {
		if err := s.fillWithNotSeenShards(ctx, shards, parent); err != nil {
			return err
		}
	}

	return nil
}

func (s *Scanner) addBlock(master *ton.BlockIDExt, txDB *gorm.DB) error {
	b := storage.Block{
		SeqNo:       master.SeqNo,
		Workchain:   master.Workchain,
		Shard:       master.Shard,
		ProcessedAt: time.Now(),
	}

	if err := txDB.Create(b).Error; err != nil {
		return err
	}

	s.lastBlock = b
	s.lastBlock.SeqNo++

	return nil
}

func (s *Scanner) getLastBlockSeqno(ctx context.Context) (uint32, error) {
	lastMaster, err := s.api.GetMasterchainInfo(ctx)
	if err != nil {
		return 0, err
	}

	return lastMaster.SeqNo, nil
}
