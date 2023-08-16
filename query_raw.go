package redisearch

import (
	"strconv"
	"strings"

	"github.com/latolukasz/beeorm/v2"
)

func GetEntityIDs(redisSearch *RedisSearch, index string, q *RedisSearchQuery, pager *beeorm.Pager) ([]uint64, uint64) {
	total, keys := redisSearch.SearchKeys(index, q, pager)
	if total == 0 || len(keys) == 0 {
		return nil, 0
	}

	ids := make([]uint64, len(keys))

	for i, key := range keys {
		id, err := strconv.ParseUint(strings.Split(key, ":")[1], 10, 64)
		if err != nil {
			return nil, 0
		}

		ids[i] = id
	}

	return ids, total
}
