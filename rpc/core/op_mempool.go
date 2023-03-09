package core

import (
	"fmt"

	abci "github.com/tendermint/tendermint/abci/types"
	opmempl "github.com/tendermint/tendermint/eip4337/mempool"
	ctypes "github.com/tendermint/tendermint/rpc/core/types"
	rpctypes "github.com/tendermint/tendermint/rpc/jsonrpc/types"
	"github.com/tendermint/tendermint/types"
)

func BroadcastOpAsync(ctx *rpctypes.Context, op types.Op) (*ctypes.ResultBroadcastOp, error) {
	err := env.OpMempool.CheckOp(op, nil, opmempl.OpInfo{})
	if err != nil {
		return nil, err
	}
	return &ctypes.ResultBroadcastOp{Hash: op.Hash()}, nil
}

func BroadcastOpSync(ctx *rpctypes.Context, op types.Op) (*ctypes.ResultBroadcastOp, error) {
	resCh := make(chan *abci.Response, 1)
	err := env.OpMempool.CheckOp(op, func(res *abci.Response) {
		select {
		case <-ctx.Context().Done():
		case resCh <- res:
		}

	}, opmempl.OpInfo{})
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Context().Done():
		return nil, fmt.Errorf("broadcast confirmation not received: %w", ctx.Context().Err())
	case res := <-resCh:
		r := res.GetCheckOp()
		return &ctypes.ResultBroadcastOp{
			Code:      r.Code,
			Codespace: r.Codespace,
			Ret:       r.Ret,
			Hash:      op.Hash(),
		}, nil
	}
}
