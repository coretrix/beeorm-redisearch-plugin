package redisearch

import (
	"strconv"

	"github.com/pkg/errors"
)

// HandleRedisIndexerEvent : put this in your consumer from stream "RedisSearchIndexerChannel"
func (r *Engine) HandleRedisIndexerEvent(indexName string) {
	var indexDefinition *RedisSearchIndex

	val, has := r.redisSearchIndices[indexName]
	if has {
		indexDefinition = val
	}

	if indexDefinition == nil {
		return
	}

	pusher := NewRedisSearchIndexPusher(r.engine, r.pool)

	id := uint64(0)
	idRedisKey := redisSearchForceIndexLastIDKeyPrefix + indexName
	idInRedis, has := r.redis.Get(idRedisKey)

	if has {
		id, _ = strconv.ParseUint(idInRedis, 10, 64)
	}

	for {
		hasMore := false
		nextID := uint64(0)

		if indexDefinition.Indexer != nil {
			newID, hasNext := indexDefinition.Indexer(r.engine, id, pusher)
			hasMore = hasNext
			nextID = newID

			pusher.Flush()

			if hasMore {
				r.redis.Set(idRedisKey, strconv.FormatUint(nextID, 10), 86400)
			}
		}

		if !hasMore {
			r.redis.Del(idRedisKey)

			break
		}

		if nextID <= id {
			panic(errors.Errorf("loop detected in indexer for index %s in pool %s", indexDefinition.Name, r.pool))
		}

		id = nextID
	}
}

type IndexerEventRedisearch struct {
	Index string
}
