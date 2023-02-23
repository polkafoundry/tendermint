package v0

import (
	"bytes"
	"errors"
	"sync"
	"sync/atomic"

	abci "github.com/tendermint/tendermint/abci/types"
	"github.com/tendermint/tendermint/config"
	"github.com/tendermint/tendermint/eip4337/mempool"
	"github.com/tendermint/tendermint/libs/clist"
	"github.com/tendermint/tendermint/libs/log"
	tmmath "github.com/tendermint/tendermint/libs/math"
	tmsync "github.com/tendermint/tendermint/libs/sync"
	"github.com/tendermint/tendermint/p2p"
	"github.com/tendermint/tendermint/proxy"
	"github.com/tendermint/tendermint/types"
)

// CListMempool is an ordered in-memory pool for transactions before they are
// proposed in a consensus round. Transaction validity is checked using the
// CheckOp abci message before the transaction is added to the pool. The
// mempool uses a concurrent list structure for storing transactions that can
// be efficiently accessed by multiple concurrent readers.
type CListMempool struct {
	// Atomic integers
	height   int64 // the last block Update()'d to
	opsBytes int64 // total size of mempool, in bytes

	// notify listeners (ie. consensus) when ops are available
	notifiedOpsAvailable bool
	opsAvailable         chan struct{} // fires once for each height, when the mempool is not empty

	config *config.OpMempoolConfig

	// Exclusive mutex for Update method to prevent concurrent execution of
	// CheckOp or ReapMaxBytesMaxGas(ReapMaxOps) methods.
	updateMtx tmsync.RWMutex
	preCheck  mempool.PreCheckFunc
	postCheck mempool.PostCheckFunc

	ops          *clist.CList // concurrent linked-list of good ops
	proxyAppConn proxy.AppConnOpMempool

	// Track whether we're rechecking ops.
	// These are not protected by a mutex and are expected to be mutated in
	// serial (ie. by abci responses which are called in serial).
	recheckCursor *clist.CElement // next expected response
	recheckEnd    *clist.CElement // re-checking stops here

	// Map for quick access to ops to record sender in CheckOp.
	// opsMap: opKey -> CElement
	opsMap sync.Map

	// Keep a cache of already-seen ops.
	// This reduces the pressure on the proxyApp.
	cache mempool.OpCache

	logger  log.Logger
	metrics *mempool.Metrics
}

var _ mempool.Mempool = &CListMempool{}

// CListMempoolOption sets an optional parameter on the mempool.
type CListMempoolOption func(*CListMempool)

// NewCListMempool returns a new mempool with the given configuration and
// connection to an application.
func NewCListMempool(
	cfg *config.OpMempoolConfig,
	proxyAppConn proxy.AppConnOpMempool,
	height int64,
	options ...CListMempoolOption,
) *CListMempool {

	mp := &CListMempool{
		config:        cfg,
		proxyAppConn:  proxyAppConn,
		ops:           clist.New(),
		height:        height,
		recheckCursor: nil,
		recheckEnd:    nil,
		logger:        log.NewNopLogger(),
		metrics:       mempool.NopMetrics(),
	}

	if cfg.CacheSize > 0 {
		mp.cache = mempool.NewLRUOpCache(cfg.CacheSize)
	} else {
		mp.cache = mempool.NopOpCache{}
	}

	proxyAppConn.SetResponseCallback(mp.globalCb)

	for _, option := range options {
		option(mp)
	}

	return mp
}

// NOTE: not thread safe - should only be called once, on startup
func (mem *CListMempool) EnableOpsAvailable() {
	mem.opsAvailable = make(chan struct{}, 1)
}

// SetLogger sets the Logger.
func (mem *CListMempool) SetLogger(l log.Logger) {
	mem.logger = l
}

// WithPreCheck sets a filter for the mempool to reject a op if f(op) returns
// false. This is ran before CheckOp. Only applies to the first created block.
// After that, Update overwrites the existing value.
func WithPreCheck(f mempool.PreCheckFunc) CListMempoolOption {
	return func(mem *CListMempool) { mem.preCheck = f }
}

// WithPostCheck sets a filter for the mempool to reject a op if f(op) returns
// false. This is ran after CheckOp. Only applies to the first created block.
// After that, Update overwrites the existing value.
func WithPostCheck(f mempool.PostCheckFunc) CListMempoolOption {
	return func(mem *CListMempool) { mem.postCheck = f }
}

// WithMetrics sets the metrics.
func WithMetrics(metrics *mempool.Metrics) CListMempoolOption {
	return func(mem *CListMempool) { mem.metrics = metrics }
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) Lock() {
	mem.updateMtx.Lock()
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) Unlock() {
	mem.updateMtx.Unlock()
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) Size() int {
	return mem.ops.Len()
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) SizeBytes() int64 {
	return atomic.LoadInt64(&mem.opsBytes)
}

// Lock() must be help by the caller during execution.
func (mem *CListMempool) FlushAppConn() error {
	return mem.proxyAppConn.FlushSync()
}

// XXX: Unsafe! Calling Flush may leave mempool in inconsistent state.
func (mem *CListMempool) Flush() {
	mem.updateMtx.RLock()
	defer mem.updateMtx.RUnlock()

	_ = atomic.SwapInt64(&mem.opsBytes, 0)
	mem.cache.Reset()

	for e := mem.ops.Front(); e != nil; e = e.Next() {
		mem.ops.Remove(e)
		e.DetachPrev()
	}

	mem.opsMap.Range(func(key, _ interface{}) bool {
		mem.opsMap.Delete(key)
		return true
	})
}

// OpsFront returns the first transaction in the ordered list for peer
// goroutines to call .NextWait() on.
// FIXME: leaking implementation details!
//
// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) OpsFront() *clist.CElement {
	return mem.ops.Front()
}

// OpsWaitChan returns a channel to wait on transactions. It will be closed
// once the mempool is not empty (ie. the internal `mem.ops` has at least one
// element)
//
// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) OpsWaitChan() <-chan struct{} {
	return mem.ops.WaitChan()
}

// It blocks if we're waiting on Update() or Reap().
// cb: A callback from the CheckOp command.
//
//	It gets called from another goroutine.
//
// CONTRACT: Either cb will get called, or err returned.
//
// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) CheckOp(
	op types.Op,
	cb func(*abci.Response),
	opInfo mempool.OpInfo,
) error {

	mem.updateMtx.RLock()
	// use defer to unlock mutex because application (*local client*) might panic
	defer mem.updateMtx.RUnlock()

	opSize := len(op)

	if err := mem.isFull(opSize); err != nil {
		return err
	}

	if opSize > mem.config.MaxOpBytes {
		return mempool.ErrOpTooLarge{
			Max:    mem.config.MaxOpBytes,
			Actual: opSize,
		}
	}

	if mem.preCheck != nil {
		if err := mem.preCheck(op); err != nil {
			return mempool.ErrPreCheck{
				Reason: err,
			}
		}
	}

	// NOTE: proxyAppConn may error if op buffer is full
	if err := mem.proxyAppConn.Error(); err != nil {
		return err
	}

	if !mem.cache.Push(op) { // if the transaction already exists in the cache
		// Record a new sender for a op we've already seen.
		// Note it's possible a op is still in the cache but no longer in the mempool
		// (eg. after committing a block, ops are removed from mempool but not cache),
		// so we only record the sender for ops still in the mempool.
		if e, ok := mem.opsMap.Load(op.Key()); ok {
			memOp := e.(*clist.CElement).Value.(*mempoolOp)
			memOp.senders.LoadOrStore(opInfo.SenderID, true)
			// TODO: consider punishing peer for dups,
			// its non-trivial since invalid ops can become valid,
			// but they can spam the same op with little cost to them atm.
		}
		return mempool.ErrOpInCache
	}

	reqRes := mem.proxyAppConn.CheckOpAsync(abci.RequestCheckOp{Op: op})
	reqRes.SetCallback(mem.reqResCb(op, opInfo.SenderID, opInfo.SenderP2PID, cb))

	return nil
}

// Global callback that will be called after every ABCI response.
// Having a single global callback avoids needing to set a callback for each request.
// However, processing the checkOp response requires the peerID (so we can track which ops we heard from who),
// and peerID is not included in the ABCI request, so we have to set request-specific callbacks that
// include this information. If we're not in the midst of a recheck, this function will just return,
// so the request specific callback can do the work.
//
// When rechecking, we don't need the peerID, so the recheck callback happens
// here.
func (mem *CListMempool) globalCb(req *abci.Request, res *abci.Response) {
	if mem.recheckCursor == nil {
		return
	}

	mem.metrics.RecheckTimes.Add(1)
	mem.resCbRecheck(req, res)

	// update metrics
	mem.metrics.Size.Set(float64(mem.Size()))
}

// Request specific callback that should be set on individual reqRes objects
// to incorporate local information when processing the response.
// This allows us to track the peer that sent us this op, so we can avoid sending it back to them.
// NOTE: alternatively, we could include this information in the ABCI request itself.
//
// External callers of CheckOp, like the RPC, can also pass an externalCb through here that is called
// when all other response processing is complete.
//
// Used in CheckOp to record PeerID who sent us the op.
func (mem *CListMempool) reqResCb(
	op []byte,
	peerID uint16,
	peerP2PID p2p.ID,
	externalCb func(*abci.Response),
) func(res *abci.Response) {
	return func(res *abci.Response) {
		if mem.recheckCursor != nil {
			// this should never happen
			panic("recheck cursor is not nil in reqResCb")
		}

		mem.resCbFirstTime(op, peerID, peerP2PID, res)

		// update metrics
		mem.metrics.Size.Set(float64(mem.Size()))

		// passed in by the caller of CheckOp, eg. the RPC
		if externalCb != nil {
			externalCb(res)
		}
	}
}

// Called from:
//   - resCbFirstTime (lock not held) if op is valid
func (mem *CListMempool) addOp(memOp *mempoolOp) {
	e := mem.ops.PushBack(memOp)
	mem.opsMap.Store(memOp.op.Key(), e)
	atomic.AddInt64(&mem.opsBytes, int64(len(memOp.op)))
	mem.metrics.OpSizeBytes.Observe(float64(len(memOp.op)))
}

// Called from:
//   - Update (lock held) if op was committed
//   - resCbRecheck (lock not held) if op was invalidated
func (mem *CListMempool) removeOp(op types.Op, elem *clist.CElement, removeFromCache bool) {
	mem.ops.Remove(elem)
	elem.DetachPrev()
	mem.opsMap.Delete(op.Key())
	atomic.AddInt64(&mem.opsBytes, int64(-len(op)))

	if removeFromCache {
		mem.cache.Remove(op)
	}
}

// RemoveOpByKey removes a transaction from the mempool by its OpKey index.
func (mem *CListMempool) RemoveOpByKey(opKey types.OpKey) error {
	if e, ok := mem.opsMap.Load(opKey); ok {
		memOp := e.(*clist.CElement).Value.(*mempoolOp)
		if memOp != nil {
			mem.removeOp(memOp.op, e.(*clist.CElement), false)
			return nil
		}
		return errors.New("transaction not found")
	}
	return errors.New("invalid transaction found")
}

func (mem *CListMempool) isFull(opSize int) error {
	var (
		memSize  = mem.Size()
		opsBytes = mem.SizeBytes()
	)

	if memSize >= mem.config.Size || int64(opSize)+opsBytes > mem.config.MaxOpsBytes {
		return mempool.ErrMempoolIsFull{
			NumOps:      memSize,
			MaxOps:      mem.config.Size,
			OpsBytes:    opsBytes,
			MaxOpsBytes: mem.config.MaxOpsBytes,
		}
	}

	return nil
}

// callback, which is called after the app checked the op for the first time.
//
// The case where the app checks the op for the second and subsequent times is
// handled by the resCbRecheck callback.
func (mem *CListMempool) resCbFirstTime(
	op []byte,
	peerID uint16,
	peerP2PID p2p.ID,
	res *abci.Response,
) {
	switch r := res.Value.(type) {
	case *abci.Response_CheckOp:
		var postCheckErr error
		if mem.postCheck != nil {
			postCheckErr = mem.postCheck(op, r.CheckOp)
		}
		if (r.CheckOp.Code == abci.CodeTypeOK) && postCheckErr == nil {
			// Check mempool isn't full again to reduce the chance of exceeding the
			// limits.
			if err := mem.isFull(len(op)); err != nil {
				// remove from cache (mempool might have a space later)
				mem.cache.Remove(op)
				mem.logger.Error(err.Error())
				return
			}

			memOp := &mempoolOp{
				height:    mem.height,
				gasWanted: r.CheckOp.GasWanted,
				op:        op,
			}
			memOp.senders.Store(peerID, true)
			mem.addOp(memOp)
			mem.logger.Debug(
				"added good operation",
				"tx", types.Op(op).Hash(),
				"res", r,
				"height", memOp.height,
				"total", mem.Size(),
			)
			mem.notifyOpsAvailable()
		} else {
			// ignore bad operation
			mem.logger.Debug(
				"rejected bad operation",
				"op", types.Op(op).Hash(),
				"peerID", peerP2PID,
				"res", r,
				"err", postCheckErr,
			)
			mem.metrics.FailedOps.Add(1)

			if !mem.config.KeepInvalidOpsInCache {
				// remove from cache (it might be good later)
				mem.cache.Remove(op)
			}
		}
	default:
		// ignore other messages
	}
}

// callback, which is called after the app rechecked the op.
//
// The case where the app checks the op for the first time is handled by the
// resCbFirstTime callback.
func (mem *CListMempool) resCbRecheck(req *abci.Request, res *abci.Response) {
	switch r := res.Value.(type) {
	case *abci.Response_CheckOp:
		op := req.GetCheckOp().Op
		memOp := mem.recheckCursor.Value.(*mempoolOp)

		// Search through the remaining list of op to recheck for a transaction that matches
		// the one we received from the ABCI application.
		for {
			if bytes.Equal(op, memOp.op) {
				// We've found an op in the recheck list that matches the op that we
				// received from the ABCI application.
				// Break, and use this transaction for further checks.
				break
			}

			mem.logger.Error(
				"re-CheckOp operation mismatch",
				"got", types.Op(op),
				"expected", memOp.op,
			)

			if mem.recheckCursor == mem.recheckEnd {
				// we reached the end of the recheckTx list without finding a op
				// matching the one we received from the ABCI application.
				// Return without processing any op.
				mem.recheckCursor = nil
				return
			}

			mem.recheckCursor = mem.recheckCursor.Next()
			memOp = mem.recheckCursor.Value.(*mempoolOp)
		}

		var postCheckErr error
		if mem.postCheck != nil {
			postCheckErr = mem.postCheck(op, r.CheckOp)
		}

		if (r.CheckOp.Code == abci.CodeTypeOK) && postCheckErr == nil {
			// Good, nothing to do.
		} else {
			// Tx became invalidated due to newly committed block.
			mem.logger.Debug("op is no longer valid", "op", types.Tx(op).Hash(), "res", r, "err", postCheckErr)
			// NOTE: we remove op from the cache because it might be good later
			mem.removeOp(op, mem.recheckCursor, !mem.config.KeepInvalidOpsInCache)
		}
		if mem.recheckCursor == mem.recheckEnd {
			mem.recheckCursor = nil
		} else {
			mem.recheckCursor = mem.recheckCursor.Next()
		}
		if mem.recheckCursor == nil {
			// Done!
			mem.logger.Debug("done rechecking txs")

			// incase the recheck removed all txs
			if mem.Size() > 0 {
				mem.notifyOpsAvailable()
			}
		}
	default:
		// ignore other messages
	}
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) OpsAvailable() <-chan struct{} {
	return mem.opsAvailable
}

func (mem *CListMempool) notifyOpsAvailable() {
	if mem.Size() == 0 {
		panic("notified ops available but mempool is empty!")
	}
	if mem.opsAvailable != nil && !mem.notifiedOpsAvailable {
		// channel cap is 1, so this will send once
		mem.notifiedOpsAvailable = true
		select {
		case mem.opsAvailable <- struct{}{}:
		default:
		}
	}
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) ReapMaxBytesMaxGas(maxBytes, maxGas int64) types.Ops {
	mem.updateMtx.RLock()
	defer mem.updateMtx.RUnlock()

	var (
		totalGas    int64
		runningSize int64
	)

	// TODO: we will get a performance boost if we have a good estimate of avg
	// size per op, and set the initial capacity based off of that.
	// ops := make([]types.Op, 0, tmmath.MinInt(mem.ops.Len(), max/mem.avgOpSize))
	ops := make([]types.Op, 0, mem.ops.Len())
	for e := mem.ops.Front(); e != nil; e = e.Next() {
		memOp := e.Value.(*mempoolOp)

		ops = append(ops, memOp.op)

		dataSize := types.ComputeProtoSizeForOps([]types.Op{memOp.op})

		// Check total size requirement
		if maxBytes > -1 && runningSize+dataSize > maxBytes {
			return ops[:len(ops)-1]
		}

		runningSize += dataSize

		// Check total gas requirement.
		// If maxGas is negative, skip this check.
		// Since newTotalGas < masGas, which
		// must be non-negative, it follows that this won't overflow.
		newTotalGas := totalGas + memOp.gasWanted
		if maxGas > -1 && newTotalGas > maxGas {
			return ops[:len(ops)-1]
		}
		totalGas = newTotalGas
	}
	return ops
}

// Safe for concurrent use by multiple goroutines.
func (mem *CListMempool) ReapMaxOps(max int) types.Ops {
	mem.updateMtx.RLock()
	defer mem.updateMtx.RUnlock()

	if max < 0 {
		max = mem.ops.Len()
	}

	ops := make([]types.Op, 0, tmmath.MinInt(mem.ops.Len(), max))
	for e := mem.ops.Front(); e != nil && len(ops) <= max; e = e.Next() {
		memOp := e.Value.(*mempoolOp)
		ops = append(ops, memOp.op)
	}
	return ops
}

// Lock() must be help by the caller during execution.
func (mem *CListMempool) Update(
	height int64,
	ops types.Ops,
	deliverTxResponses []*abci.ResponseDeliverOp,
	preCheck mempool.PreCheckFunc,
	postCheck mempool.PostCheckFunc,
) error {
	// Set height
	mem.height = height
	mem.notifiedOpsAvailable = false

	if preCheck != nil {
		mem.preCheck = preCheck
	}
	if postCheck != nil {
		mem.postCheck = postCheck
	}

	for i, op := range ops {
		if deliverTxResponses[i].Code == abci.CodeTypeOK {
			// Add valid committed tx to the cache (if missing).
			_ = mem.cache.Push(op)
		} else if !mem.config.KeepInvalidOpsInCache {
			// Allow invalid transactions to be resubmitted.
			mem.cache.Remove(op)
		}

		// Remove committed tx from the mempool.
		//
		// Note an evil proposer can drop valid txs!
		// Mempool before:
		//   100 -> 101 -> 102
		// Block, proposed by an evil proposer:
		//   101 -> 102
		// Mempool after:
		//   100
		// https://github.com/tendermint/tendermint/issues/3322.
		if e, ok := mem.opsMap.Load(op.Key()); ok {
			mem.removeOp(op, e.(*clist.CElement), false)
		}
	}

	// Either recheck non-committed txs to see if they became invalid
	// or just notify there're some txs left.
	if mem.Size() > 0 {
		if mem.config.Recheck {
			mem.logger.Debug("recheck txs", "numtxs", mem.Size(), "height", height)
			mem.recheckOps()
			// At this point, mem.txs are being rechecked.
			// mem.recheckCursor re-scans mem.txs and possibly removes some txs.
			// Before mem.Reap(), we should wait for mem.recheckCursor to be nil.
		} else {
			mem.notifyOpsAvailable()
		}
	}

	// Update metrics
	mem.metrics.Size.Set(float64(mem.Size()))

	return nil
}

func (mem *CListMempool) recheckOps() {
	if mem.Size() == 0 {
		panic("recheckOps is called, but the mempool is empty")
	}

	mem.recheckCursor = mem.ops.Front()
	mem.recheckEnd = mem.ops.Back()

	// Push ops to proxyAppConn
	// NOTE: globalCb may be called concurrently.
	for e := mem.ops.Front(); e != nil; e = e.Next() {
		memOp := e.Value.(*mempoolOp)
		mem.proxyAppConn.CheckOpAsync(abci.RequestCheckOp{
			Op:   memOp.op,
			Type: abci.CheckOpType_Recheck,
		})
	}

	mem.proxyAppConn.FlushAsync()
}

//--------------------------------------------------------------------------------

// mempoolOp is a transaction that successfully ran
type mempoolOp struct {
	height    int64    // height that this op had been validated in
	gasWanted int64    // amount of gas this op states it will require
	op        types.Op //

	// ids of peers who've sent us this op (as a map for quick lookups).
	// senders: PeerID -> bool
	senders sync.Map
}

// Height returns the height for this transaction
func (memOp *mempoolOp) Height() int64 {
	return atomic.LoadInt64(&memOp.height)
}
