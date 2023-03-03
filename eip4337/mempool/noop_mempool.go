package mempool

import (
	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/types"
)

type NoopMempool struct{}

func (mem *NoopMempool) CheckOp(op types.Op, callback func(*abci.Response), opInfo OpInfo) error {
	return nil
}

func (mem *NoopMempool) RemoveOpByKey(opKey types.OpKey) error {
	return nil
}

func (mem *NoopMempool) ReapMaxBytesMaxGas(maxBytes, maxGas int64) types.Ops {
	return types.Ops{}
}

func (mem *NoopMempool) ReapMaxOps(max int) types.Ops {
	return types.Ops{}
}

func (mem *NoopMempool) Lock() {}

func (mem *NoopMempool) Unlock() {}

func (mem *NoopMempool) Update(height int64, ops types.Ops, deliverTxResponses []*abci.ResponseDeliverOp, preCheck PreCheckFunc, postCheck PostCheckFunc) error {
	return nil
}

func (mem *NoopMempool) FlushAppConn() error { return nil }

func (mem *NoopMempool) Flush() {}

func (mem *NoopMempool) OpsAvailable() <-chan struct{} {
	return nil
}

func (mem *NoopMempool) EnableOpsAvailable() {}

func (mem *NoopMempool) Size() int { return 0 }

func (mem *NoopMempool) SizeBytes() int64 { return 0 }

var _ Mempool = (*NoopMempool)(nil)
