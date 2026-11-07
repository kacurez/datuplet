package datagateway

import "sync"

// rowCap enforces a preview run's per-output-table row limit. It is shared by
// every writer of the gateway, so the limit holds per (bucket, table) however
// many writers feed it.
type rowCap struct {
	mu    sync.Mutex
	used  map[string]int64
	limit int64
}

func newRowCap(limit int64) *rowCap {
	return &rowCap{used: make(map[string]int64), limit: limit}
}

// take atomically grants min(n, limit-used) rows for the table. reached is
// true when the table is full after the grant (including n == 0 on a full
// table). A nil rowCap grants n and never reaches a limit.
func (c *rowCap) take(bucket, table string, n int64) (allowed int64, reached bool) {
	if c == nil {
		return n, false
	}
	key := bucket + "\x00" + table
	c.mu.Lock()
	defer c.mu.Unlock()
	used := c.used[key]
	allowed = c.limit - used
	if allowed > n {
		allowed = n
	}
	if allowed < 0 {
		allowed = 0
	}
	used += allowed
	c.used[key] = used
	return allowed, used >= c.limit
}
