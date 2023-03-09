package mempool

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"math"

	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/types"
)

const (
	MempoolChannel = byte(0x31)

	// PeerCatchupSleepIntervalMS defines how much time to sleep if a peer is behind
	PeerCatchupSleepIntervalMS = 100

	// UnknownPeerID is the peer ID to use when running CheckOp when there is
	// no peer (e.g. RPC)
	UnknownPeerID uint16 = 0

	MaxActiveIDs = math.MaxUint16
)

// Mempool defines the mempool interface.
//
// Updates to the mempool need to be synchronized with committing a block so
// applications can reset their transient state on Commit.
type Mempool interface {
	// CheckOp executes a new transaction against the application to determine
	// its validity and whether it should be added to the mempool.
	CheckOp(op types.Op, callback func(*abci.Response), opInfo OpInfo) error

	// RemoveOpByKey removes a transaction, identified by its key,
	// from the mempool.
	RemoveOpByKey(opKey types.OpKey) error

	// ReapMaxBytesMaxGas reaps transactions from the mempool up to maxBytes
	// bytes total with the condition that the total gasWanted must be less than
	// maxGas.
	//
	// If both maxes are negative, there is no cap on the size of all returned
	// transactions (~ all available transactions).
	ReapMaxBytesMaxGas(maxBytes, maxGas int64) types.Ops

	// ReapMaxOps reaps up to max transactions from the mempool. If max is
	// negative, there is no cap on the size of all returned transactions
	// (~ all available transactions).
	ReapMaxOps(max int) types.Ops

	// Lock locks the mempool. The consensus must be able to hold lock to safely
	// update.
	Lock()

	// Unlock unlocks the mempool.
	Unlock()

	// Update informs the mempool that the given ops were committed and can be
	// discarded.
	//
	// NOTE:
	// 1. This should be called *after* block is committed by consensus.
	// 2. Lock/Unlock must be managed by the caller.
	Update(height int64,
		ops types.Ops,
		deliverOpResponses []*abci.ResponseDeliverOp,
		preCheck PreCheckFunc,
		postCheck PostCheckFunc) error

	// FlushAppConn flushes the mempool connection to ensure async callback calls
	// are done, e.g. from CheckOp.
	//
	// NOTE:
	// 1. Lock/Unlock must be managed by caller.
	FlushAppConn() error

	// Flush removes all transactions from the mempool and caches.
	Flush()

	// OpsAvailable returns a channel which fires once for every height, and only
	// when transactions are available in the mempool.
	//
	// NOTE:
	// 1. The returned channel may be nil if EnableOpsAvailable was not called.
	OpsAvailable() <-chan struct{}

	// EnableOpsAvailable initializes the OpsAvailable channel, ensuring it will
	// trigger once every height when transactions are available.
	EnableOpsAvailable()

	// Size returns the number of transactions in the mempool.
	Size() int

	// SizeBytes returns the total size of all ops in the mempool.
	SizeBytes() int64
}

// PreCheckFunc is an optional filter executed before CheckOp and rejects
// transaction if false is returned. An example would be to ensure that a
// transaction doesn't exceeded the block size.
type PreCheckFunc func(types.Op) error

// PostCheckFunc is an optional filter executed after CheckOp and rejects
// transaction if false is returned. An example would be to ensure a
// transaction doesn't require more gas than available for the block.
type PostCheckFunc func(types.Op, *abci.ResponseCheckOp) error

// PreCheckMaxBytes checks that the size of the transaction is smaller or equal
// to the expected maxBytes.
func PreCheckMaxBytes(maxBytes int64) PreCheckFunc {
	return func(op types.Op) error {
		opSize := types.ComputeProtoSizeForOps([]types.Op{op})

		if opSize > maxBytes {
			return fmt.Errorf("op size is too big: %d, max: %d", opSize, maxBytes)
		}

		return nil
	}
}

// PostCheckMaxGas checks that the wanted gas is smaller or equal to the passed
// maxGas. Returns nil if maxGas is -1.
func PostCheckMaxGas(maxGas int64) PostCheckFunc {
	return func(op types.Op, res *abci.ResponseCheckOp) error {
		if maxGas == -1 {
			return nil
		}
		if res.GasWanted < 0 {
			return fmt.Errorf("gas wanted %d is negative",
				res.GasWanted)
		}
		if res.GasWanted > maxGas {
			return fmt.Errorf("gas wanted %d is greater than max gas %d",
				res.GasWanted, maxGas)
		}

		return nil
	}
}

// ErrOpInCache is returned to the client if we saw op earlier
var ErrOpInCache = errors.New("op already exists in cache")

// OpKey is the fixed length array key used as an index.
type OpKey [sha256.Size]byte

// ErrOpTooLarge defines an error when a transaction is too big to be sent in a
// message to other peers.
type ErrOpTooLarge struct {
	Max    int
	Actual int
}

func (e ErrOpTooLarge) Error() string {
	return fmt.Sprintf("Op too large. Max size is %d, but got %d", e.Max, e.Actual)
}

// ErrMempoolIsFull defines an error where Tendermint and the application cannot
// handle that much load.
type ErrMempoolIsFull struct {
	NumOps      int
	MaxOps      int
	OpsBytes    int64
	MaxOpsBytes int64
}

func (e ErrMempoolIsFull) Error() string {
	return fmt.Sprintf(
		"mempool is full: number of ops %d (max: %d), total ops bytes %d (max: %d)",
		e.NumOps,
		e.MaxOps,
		e.OpsBytes,
		e.MaxOpsBytes,
	)
}

// ErrPreCheck defines an error where a transaction fails a pre-check.
type ErrPreCheck struct {
	Reason error
}

func (e ErrPreCheck) Error() string {
	return e.Reason.Error()
}

// IsPreCheckError returns true if err is due to pre check failure.
func IsPreCheckError(err error) bool {
	return errors.As(err, &ErrPreCheck{})
}
