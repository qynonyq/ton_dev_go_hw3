package scanner

import (
	"context"
	"time"

	"github.com/qynonyq/ton_dev_go_hw3/internal/app"
	"github.com/qynonyq/ton_dev_go_hw3/internal/storage"
	"github.com/sirupsen/logrus"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/ton"
)

type Scanner struct {
	api             *ton.APIClient
	lastBlock       storage.Block
	lastShardsSeqNo map[string]uint32
	Client          *liteclient.ConnectionPool
}

func NewScanner(ctx context.Context, cfg *liteclient.GlobalConfig) (*Scanner, error) {
	client := liteclient.NewConnectionPool()
	if err := client.AddConnectionsFromConfigUrl(ctx, app.MainnetCfgURL); err != nil {
		return nil, err
	}
	api := ton.NewAPIClient(client)

	return &Scanner{
		api:             api,
		lastBlock:       storage.Block{},
		lastShardsSeqNo: make(map[string]uint32),
		Client:          client,
	}, nil
}

func (s *Scanner) Stop() {
	s.Client.Stop()
}

func (s *Scanner) Listen(ctx context.Context) {
	logrus.Info("[SCN] start scanning blocks")

	err := app.DB.Last(&s.lastBlock).Error
	if err == nil {
		// process next block
		s.lastBlock.SeqNo++
	}
	if err != nil {
		// get last block from MC
		lastMaster, err := s.api.GetMasterchainInfo(ctx)
		for err != nil {
			time.Sleep(time.Second)
			logrus.Error("[SCN] error when get last master: ", err)
			lastMaster, err = s.api.GetMasterchainInfo(ctx)
		}

		s.lastBlock.SeqNo = lastMaster.SeqNo
		s.lastBlock.Shard = lastMaster.Shard
		s.lastBlock.Workchain = lastMaster.Workchain
	}

	master, err := s.api.LookupBlock(
		ctx,
		s.lastBlock.Workchain,
		s.lastBlock.Shard,
		s.lastBlock.SeqNo,
	)
	for err != nil {
		logrus.Error("[SCN] failed to lookup master block: ", err)
		time.Sleep(time.Second)
		master, err = s.api.LookupBlock(
			ctx,
			s.lastBlock.Workchain,
			s.lastBlock.Shard,
			s.lastBlock.SeqNo,
		)
	}

	firstShards, err := s.api.GetBlockShardsInfo(ctx, master)
	for err != nil {
		logrus.Error("[SCN] failed to get first shards: ", err)
		time.Sleep(time.Second)
	}

	for _, shard := range firstShards {
		s.lastShardsSeqNo[s.getShardID(shard)] = shard.SeqNo
	}

	s.processBlocks(ctx)
}
