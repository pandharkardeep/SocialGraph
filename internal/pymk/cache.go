package pymk

import (
	"container/list"
	"time"
)

type cacheKey struct {
	user   uint64
	k      int
	epoch  uint64 // user's epoch at time of compute (invalidates on change)
}

type cacheEntry struct {
	key       cacheKey
	value     []Suggestion
	expiresAt time.Time
}

type lruCache struct {
	capacity int
	ttl      time.Duration
	ll       *list.List
	table    map[cacheKey]*list.Element
	onEvict  func()
	onHit    func()
	onMiss   func()
}

func newLRU(cap int, ttl time.Duration) *lruCache {
	return &lruCache{
		capacity: cap,
		ttl:      ttl,
		ll:       list.New(),
		table:    make(map[cacheKey]*list.Element),
	}
}

func (c *lruCache) Get(key cacheKey) (val []Suggestion, ok bool) {
	if c.capacity == 0 { return nil, false }
	if ele, ok := c.table[key]; ok {
		ent := ele.Value.(*cacheEntry)
		if time.Now().After(ent.expiresAt) {
			c.removeElement(ele)
			if c.onMiss != nil { c.onMiss() }
			return nil, false
		}
		c.ll.MoveToFront(ele)
		if c.onHit != nil { c.onHit() }
		return ent.value, true
	}
	if c.onMiss != nil { c.onMiss() }
	return nil, false
}

func (c *lruCache) Set(key cacheKey, val []Suggestion) {
	if c.capacity == 0 { return }
	if ele, ok := c.table[key]; ok {
		ent := ele.Value.(*cacheEntry)
		ent.value = val
		ent.expiresAt = time.Now().Add(c.ttl)
		c.ll.MoveToFront(ele)
		return
	}
	ent := &cacheEntry{key: key, value: val, expiresAt: time.Now().Add(c.ttl)}
	ele := c.ll.PushFront(ent)
	c.table[key] = ele
	if c.ll.Len() > c.capacity {
		c.removeOldest()
	}
}

func (c *lruCache) removeOldest() {
	ele := c.ll.Back()
	if ele != nil {
		c.removeElement(ele)
		if c.onEvict != nil { c.onEvict() }
	}
}

func (c *lruCache) removeElement(e *list.Element) {
	ent := e.Value.(*cacheEntry)
	delete(c.table, ent.key)
	c.ll.Remove(e)
}
