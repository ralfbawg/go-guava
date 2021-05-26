package concurrentmap

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/ralfbawg/go-guava/hash"
)

// ShardCount 默认分区数
var ShardCount = 1 << 5

const (
	// ShardDefaultSize 单分区初始化大小
	ShardDefaultSize = 1000
	// ShardDeleteThreshold 刷新重建分区map的删除阈值（在大数量情况下可能减少FullGC or stw）
	ShardDeleteThreshold uint8 = 80
)

// ConcMap Shared A "thread" safe map of type string:Anything.
// To avoid lock bottlenecks this map is dived to several (ShardCount) map shards.
// ConcMap 是一个高效的同步分区Map工具，比原生sync.map高5-10倍的效率
type ConcMap []*Shared

// Shared A "thread" safe string to anything map.
type Shared struct {
	items        map[string]interface{}
	deleteCount  uint8
	lastRebuild  time.Time
	sync.RWMutex // Read Write mutex, guards access to internal map.
}

// NewByShardCount Creates a new concurrent map.
func NewByShardCount(shardCount int) ConcMap {
	finalShardCount := ShardCount
	if shardCount <= 0 {
		finalShardCount = shardCount
	}
	m := make(ConcMap, finalShardCount)
	for i := 0; i < finalShardCount; i++ {
		m[i] = &Shared{items: make(map[string]interface{}, ShardDefaultSize)}
	}
	return m
}

// NewConcMap Creates a new concurrent map with default shard size.
func NewConcMap() ConcMap {
	m := make(ConcMap, ShardCount)
	for i := 0; i < ShardCount; i++ {
		m[i] = &Shared{items: make(map[string]interface{}, ShardDefaultSize)}
	}
	return m
}

// GetShard returns shard by given key
func (m ConcMap) GetShard(key string) *Shared {
	return m[uint(hash.Murmur2(key))%uint(ShardCount)]
}

// MSet multi set key
func (m ConcMap) MSet(data map[string]interface{}) {
	for key, value := range data {
		shard := m.GetShard(key)
		shard.Lock()
		shard.items[key] = value
		shard.Unlock()
	}
}

// Set Sets the given value under the specified key.
func (m ConcMap) Set(key string, value interface{}) {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

// UpsertCb Callback to return new element to be inserted into the map
// It is called while lock is held, therefore it MUST NOT
// try to access other keys in same map, as it can lead to deadlock since
// Go sync.RWLock is not reentrant
type UpsertCb func(exist bool, valueInMap interface{}, newValue interface{}) interface{}

// Upsert Insert or Update - updates existing element or inserts a new one using UpsertCb
func (m ConcMap) Upsert(key string, value interface{}, cb UpsertCb) (res interface{}) {
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// SetIfAbsent Sets the given value under the specified key if no value was associated with it.
func (m ConcMap) SetIfAbsent(key string, value interface{}) bool {
	// Get map shard.
	shard := m.GetShard(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

// Get retrieves an element from map under given key.
func (m ConcMap) Get(key string) (interface{}, bool) {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// Get item from shard.
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Count returns the number of elements within the map.
func (m ConcMap) Count() int {
	count := 0
	var wg sync.WaitGroup
	wg.Add(len(m))
	for index, shard := range m {
		go func(i int, shard *Shared) {
			shard.RLock()
			defer func() {
				shard.RUnlock()
				wg.Done()
			}()
			count += len(shard.items)
		}(index, shard)
	}
	wg.Wait()

	return count
}

// Has Looks up an item under specified key
func (m ConcMap) Has(key string) bool {
	// Get shard
	shard := m.GetShard(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Remove removes an element from the map.
func (m ConcMap) Remove(key string) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.deleteCount++
	// rebuild the shard map to reduce the gc expression and the memory taken
	if (time.Until(shard.lastRebuild) > 5*time.Second && shard.
		deleteCount > ShardDeleteThreshold) || shard.deleteCount > ShardDeleteThreshold*3 { // 5秒间隔或者3倍压力时
		log.Debugf("start to rebuild map shard with deleteCount=%d", shard.deleteCount)
		tmpA := make(map[string]interface{}, len(shard.items))
		for k, v := range shard.items {
			tmpA[k] = v
		}
		shard.items = nil
		shard.items = tmpA
		shard.lastRebuild = time.Now()
		shard.deleteCount = 0
	}
	shard.Unlock()
}

// RemoveCb is a callback executed in a map.RemoveCb() call, while Lock is held
// If returns true, the element will be removed from the map
type RemoveCb func(key string, v interface{}, exists bool) bool

// RemoveCb locks the shard containing the key, retrieves its current value and calls the callback with those params
// If callback returns true and element exists, it will remove it from the map
// Returns the value returned by the callback (even if element was not present in the map)
func (m ConcMap) RemoveCb(key string, cb RemoveCb) bool {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, ok := shard.items[key]
	remove := cb(key, v, ok)
	if remove && ok {
		delete(shard.items, key)
		shard.deleteCount++
		if shard.deleteCount > ShardDeleteThreshold {
			tmpA := make(map[string]interface{})
			for k, v := range shard.items {
				tmpA[k] = v
			}
			shard.items = nil
			shard.items = tmpA
		}
	}
	shard.Unlock()
	return remove
}

// Pop removes an element from the map and returns it
func (m ConcMap) Pop(key string) (v interface{}, exists bool) {
	// Try to get shard.
	shard := m.GetShard(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// IsEmpty checks if map is empty.
func (m ConcMap) IsEmpty() bool {
	return m.Count() == 0
}

// Iter returns an iterator which could be used in a for range loop.
//
// Deprecated: using IterBuffered() will get a better performence
func (m ConcMap) Iter() <-chan Tuple {
	chans := snapshot(m)
	ch := make(chan Tuple)
	go fanIn(chans, ch)
	return ch
}

// IterBuffered returns a buffered iterator which could be used in a for range loop,u should try to using this often
func (m ConcMap) IterBuffered() <-chan Tuple {
	chans := snapshot(m)
	total := 0
	for _, c := range chans {
		total += cap(c)
	}
	ch := make(chan Tuple, total)
	go fanIn(chans, ch)
	return ch
}

// snapshot Returns a array of channels that contains elements in each shard,
// which likely takes a snapshot of `m`.
// It returns once the size of each buffered channel is determined,
// before all the channels are populated using goroutines.
func snapshot(m ConcMap) (chans []chan Tuple) {
	chans = make([]chan Tuple, ShardCount)
	wg := sync.WaitGroup{}
	wg.Add(ShardCount)
	// Foreach shard.
	for index, shard := range m {
		go func(index int, shard *Shared) {
			// Foreach key, value pair.
			shard.RLock()
			chans[index] = make(chan Tuple, len(shard.items))
			wg.Done()
			for key, val := range shard.items {
				chans[index] <- Tuple{key, val}
			}
			shard.RUnlock()
			close(chans[index])
		}(index, shard)
	}
	wg.Wait()
	return chans
}

// fanIn reads elements from channels `chans` into channel `out`
func fanIn(chans []chan Tuple, out chan Tuple) {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func(ch chan Tuple) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

// Items returns all items as map[string]interface{}
func (m ConcMap) Items() map[string]interface{} {
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

// IterCb Iterator callback,called for every key,value found in
// maps. RLock is held for all calls for a given shard
// therefore callback sess consistent view of a shard,
// but not across the shards
type IterCb func(key string, v interface{})

// IterCb Callback based iterator, cheapest way to read
// all elements in a map.
func (m ConcMap) IterCb(fn IterCb) {
	for idx := range m {
		shard := (m)[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

// Keys returns all keys as []string
func (m ConcMap) Keys() []string {
	count := m.Count()
	ch := make(chan string, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(ShardCount)
		for _, shard := range m {
			go func(shard *Shared) {
				// Foreach key, value pair.
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// Generate keys
	keys := make([]string, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

// MarshalJSON Reviles ConcMap "private" variables to json marshal.
func (m ConcMap) MarshalJSON() ([]byte, error) {
	// Create a temporary map, which will hold all item spread across shards.
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

// Tuple channel mode return via the media
type Tuple struct {
	Key string
	Val interface{}
}
