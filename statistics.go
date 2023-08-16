package redisearch

type RedisSearchStatistics struct {
	Index *RedisSearchIndex
	Info  *RedisSearchIndexInfo
}

func (r *Engine) GetRedisSearchStatistics() []*RedisSearchStatistics {
	result := make([]*RedisSearchStatistics, 0)

	for _, indexName := range r.ListIndices() {
		info := r.Info(indexName)
		index := r.GetRedisSearchIndex(indexName)

		if index == nil {
			continue
		}

		stat := &RedisSearchStatistics{Index: index, Info: info}
		result = append(result, stat)
	}

	return result
}
