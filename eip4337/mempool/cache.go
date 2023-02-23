package mempool

import (
	"container/list"

	tmsync "github.com/tendermint/tendermint/libs/sync"
	"github.com/tendermint/tendermint/types"
)

// OpCache defines an interface for raw transaction caching in a mempool.
// Currently, a OpCache does not allow direct reading or getting of transaction
// values. A OpCache is used primarily to push transactions and removing
// transactions. Pushing via Push returns a boolean telling the caller if the
// transaction already exists in the cache or not.
type OpCache interface {
	// Reset resets the cache to an empty state.
	Reset()

	// Push adds the given raw transaction to the cache and returns true if it was
	// newly added. Otherwise, it returns false.
	Push(op types.Op) bool

	// Remove removes the given raw transaction from the cache.
	Remove(op types.Op)

	// Has reports whether op is present in the cache. Checking for presence is
	// not treated as an access of the value.
	Has(op types.Op) bool
}

var _ OpCache = (*LRUOpCache)(nil)

// LRUOpCache maintains a thread-safe LRU cache of raw transactions. The cache
// only stores the hash of the raw transaction.
type LRUOpCache struct {
	mtx      tmsync.Mutex
	size     int
	cacheMap map[types.OpKey]*list.Element
	list     *list.List
}

func NewLRUOpCache(cacheSize int) *LRUOpCache {
	return &LRUOpCache{
		size:     cacheSize,
		cacheMap: make(map[types.OpKey]*list.Element, cacheSize),
		list:     list.New(),
	}
}

// GetList returns the underlying linked-list that backs the LRU cache. Note,
// this should be used for testing purposes only!
func (c *LRUOpCache) GetList() *list.List {
	return c.list
}

func (c *LRUOpCache) Reset() {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.cacheMap = make(map[types.OpKey]*list.Element, c.size)
	c.list.Init()
}

func (c *LRUOpCache) Push(op types.Op) bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	key := op.Key()

	moved, ok := c.cacheMap[key]
	if ok {
		c.list.MoveToBack(moved)
		return false
	}

	if c.list.Len() >= c.size {
		front := c.list.Front()
		if front != nil {
			frontKey := front.Value.(types.OpKey)
			delete(c.cacheMap, frontKey)
			c.list.Remove(front)
		}
	}

	e := c.list.PushBack(key)
	c.cacheMap[key] = e

	return true
}

func (c *LRUOpCache) Remove(op types.Op) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	key := op.Key()
	e := c.cacheMap[key]
	delete(c.cacheMap, key)

	if e != nil {
		c.list.Remove(e)
	}
}

func (c *LRUOpCache) Has(op types.Op) bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	_, ok := c.cacheMap[op.Key()]
	return ok
}

// NopOpCache defines a no-op raw transaction cache.
type NopOpCache struct{}

var _ OpCache = (*NopOpCache)(nil)

func (NopOpCache) Reset()             {}
func (NopOpCache) Push(types.Op) bool { return true }
func (NopOpCache) Remove(types.Op)    {}
func (NopOpCache) Has(types.Op) bool  { return false }
