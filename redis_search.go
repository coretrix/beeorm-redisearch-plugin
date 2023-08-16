package redisearch

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/latolukasz/beeorm/v2"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
)

const (
	RedisSearchNullNumber     = -math.MaxInt64
	RedisSearchIndexerChannel = "orm-redis-search-channel"

	redisSearchIndexFieldText    = "TEXT"
	redisSearchIndexFieldNumeric = "NUMERIC"
	redisSearchIndexFieldGeo     = "GEO"
	redisSearchIndexFieldTAG     = "TAG"

	redisSearchForceIndexLastIDKeyPrefix = "_orm_force_index"
)

var redisSearchIndicesInit = make(map[string]map[string]*RedisSearchIndex)
var customIndicesInit = make(map[string][]*RedisSearchIndex)

type RedisSearch struct {
	ctx                context.Context
	pool               string
	redis              beeorm.RedisCache
	engine             beeorm.Engine
	redisSearchIndices map[string]*RedisSearchIndex
}

func NewRedisSearch(ctx context.Context, engine beeorm.Engine, pool string) *RedisSearch {
	redisSearchInstance := &RedisSearch{
		ctx:                ctx,
		pool:               pool,
		engine:             engine,
		redis:              engine.GetRedis(pool),
		redisSearchIndices: map[string]*RedisSearchIndex{},
	}

	redisSearchInstance.redisSearchIndices = redisSearchIndicesInit[pool]

	if customIndices, ok := customIndicesInit[pool]; ok {
		for _, customIndex := range customIndices {
			redisSearchInstance.redisSearchIndices[customIndex.Name] = customIndex
		}
	}

	return redisSearchInstance
}

func (r *RedisSearch) GetRedisSearchIndex(indexName string) *RedisSearchIndex {
	return r.redisSearchIndices[indexName]
}

func (r *RedisSearch) ForceReindex(index string) {
	def, has := r.redisSearchIndices[index]
	if !has {
		panic(errors.Errorf("unknown index %s in pool %s", index, r.pool))
	}

	r.dropIndex(index, true)
	r.createIndex(def)

	event := redisIndexerEvent{Index: index}

	r.engine.GetEventBroker().Publish(RedisSearchIndexerChannel, event, nil)
}

func (r *RedisSearch) SearchRaw(index string, query *RedisSearchQuery, pager *beeorm.Pager) (total uint64, rows []interface{}) {
	return r.search(index, query, pager, false)
}

func (r *RedisSearch) SearchCount(index string, query *RedisSearchQuery) uint64 {
	total, _ := r.search(index, query, beeorm.NewPager(0, 0), false)

	return total
}

func (r *RedisSearch) SearchResult(index string, query *RedisSearchQuery, pager *beeorm.Pager) (total uint64, rows []*RedisSearchResult) {
	total, data := r.search(index, query, pager, false)
	rows = make([]*RedisSearchResult, 0)
	max := len(data) - 1
	i := 0

	for {
		if i > max {
			break
		}

		row := &RedisSearchResult{Key: r.redis.RemoveNamespacePrefix(data[i].(string))}

		if query.explainScore {
			i++
			row.ExplainScore = data[i].([]interface{})
			row.Score, _ = strconv.ParseFloat(row.ExplainScore[0].(string), 64)
			row.ExplainScore = row.ExplainScore[1].([]interface{})
		} else if query.withScores {
			i++
			row.Score, _ = strconv.ParseFloat(data[i].(string), 64)
		}

		i++

		row.Fields = data[i].([]interface{})
		rows = append(rows, row)

		i++
	}

	return total, rows
}

func (r *RedisSearch) SearchKeys(index string, query *RedisSearchQuery, pager *beeorm.Pager) (total uint64, keys []string) {
	total, rows := r.search(index, query, pager, true)
	keys = make([]string, len(rows))

	for k, v := range rows {
		keys[k] = r.redis.RemoveNamespacePrefix(v.(string))
	}

	return total, keys
}

func (r *RedisSearch) Aggregate(index string, query *RedisSearchAggregation, pager *beeorm.Pager) (result []map[string]string, totalRows uint64) {
	if query.query == nil {
		query.query = NewRedisSearchQuery()
	}

	index = r.redis.AddNamespacePrefix(index)
	args := []interface{}{"FT.AGGREGATE", index}
	args = r.buildQueryArgs(query.query, args)
	args = append(args, query.args...)
	args = r.applyPager(pager, args)
	cmd := redis.NewSliceCmd(r.ctx, args...)

	hasRedisLogger, redisLogger := r.engine.HasRedisLogger()

	start := getNow(hasRedisLogger)
	err := r.redis.Process(r.ctx, cmd)

	if hasRedisLogger {
		r.fillLogFields(redisLogger, "FT.AGGREGATE", cmd.String(), start, err)
	}

	checkError(err)

	res, err := cmd.Result()
	checkError(err)

	if len(res) == 1 {
		panic("redisearch aggregate timeout")
	}

	totalRows = uint64(res[0].(int64))
	result = make([]map[string]string, totalRows)

	for i, row := range res[1:] {
		data := make(map[string]string)
		rowSlice := row.([]interface{})

		for k := 0; k < len(rowSlice); k = k + 2 {
			asSLice, ok := rowSlice[k+1].([]interface{})

			if ok {
				values := make([]string, len(asSLice))

				for k, v := range asSLice {
					values[k] = v.(string)
				}

				data[rowSlice[k].(string)] = strings.Join(values, ",")
			} else {
				data[rowSlice[k].(string)] = rowSlice[k+1].(string)
			}
		}

		result[i] = data
	}

	return result, totalRows
}

func (r *RedisSearch) applyPager(pager *beeorm.Pager, args []interface{}) []interface{} {
	if pager != nil {
		if pager.PageSize > 10000 {
			panic(fmt.Errorf("pager size exceeded limit 10000"))
		}

		args = append(args, "LIMIT")
		args = append(args, (pager.CurrentPage-1)*pager.PageSize)
		args = append(args, pager.PageSize)
	} else {
		panic(fmt.Errorf("missing pager in redis search query"))
	}

	return args
}

func (r *RedisSearch) GetPoolConfig() beeorm.RedisPoolConfig {
	return r.redis.GetPoolConfig()
}

func (r *RedisSearch) search(index string, query *RedisSearchQuery, pager *beeorm.Pager, noContent bool) (total uint64, rows []interface{}) {
	index = r.redis.AddNamespacePrefix(index)
	args := []interface{}{"FT.SEARCH", index}
	args = r.buildQueryArgs(query, args)

	if noContent {
		args = append(args, "NOCONTENT")
	}

	if query.verbatim {
		args = append(args, "VERBATIM")
	}

	if query.noStopWords {
		args = append(args, "NOSTOPWORDS")
	}

	if query.withScores {
		args = append(args, "WITHSCORES")
	}

	if query.sortField != "" {
		args = append(args, "SORTBY", query.sortField)

		if query.sortDesc {
			args = append(args, "DESC")
		}
	}

	if len(query.inKeys) > 0 {
		args = append(args, "INKEYS", len(query.inKeys))

		for _, k := range query.inKeys {
			args = append(args, r.redis.AddNamespacePrefix(k.(string)))
		}
	}

	if len(query.inFields) > 0 {
		args = append(args, "INFIELDS", len(query.inFields))
		args = append(args, query.inFields...)
	}

	if len(query.toReturn) > 0 {
		args = append(args, "RETURN", len(query.toReturn))
		args = append(args, query.toReturn...)
	}

	if query.slop != 0 {
		slop := query.slop

		if slop == -1 {
			slop = 0
		}

		args = append(args, "SLOP", slop)
	}

	if query.inOrder {
		args = append(args, "INORDER")
	}

	if query.lang != "" {
		args = append(args, "LANGUAGE", query.lang)
	}

	if query.explainScore {
		args = append(args, "EXPLAINSCORE")
	}

	if query.highlight != nil {
		args = append(args, "HIGHLIGHT")

		if l := len(query.highlight); l > 0 {
			args = append(args, "FIELDS", l)
			args = append(args, query.highlight...)
		}

		if query.highlightOpenTag != "" && query.highlightCloseTag != "" {
			args = append(args, "TAGS", query.highlightOpenTag, query.highlightCloseTag)
		}
	}

	if query.summarize != nil {
		args = append(args, "SUMMARIZE")

		if l := len(query.summarize); l > 0 {
			args = append(args, "FIELDS", l)
			args = append(args, query.summarize...)
		}

		if query.summarizeFrags > 0 {
			args = append(args, "FRAGS", query.summarizeFrags)
		}

		if query.summarizeLen > 0 {
			args = append(args, "LEN", query.summarizeLen)
		}

		if query.summarizeSeparator != "" {
			args = append(args, "SEPARATOR", query.summarizeSeparator)
		}
	}

	args = r.applyPager(pager, args)
	cmd := redis.NewSliceCmd(r.ctx, args...)
	hasRedisLogger, redisLogger := r.engine.HasRedisLogger()

	start := getNow(hasRedisLogger)
	err := r.redis.Process(r.ctx, cmd)

	if hasRedisLogger {
		r.fillLogFields(redisLogger, "FT.SEARCH", cmd.String(), start, err)
	}

	checkError(err)

	res, err := cmd.Result()
	checkError(err)

	total = uint64(res[0].(int64))

	return total, res[1:]
}

func (r *RedisSearch) buildQueryArgs(query *RedisSearchQuery, args []interface{}) []interface{} {
	q := query.query

	for field, in := range query.filtersNumeric {
		if q != "" {
			q += " "
		}

		for i, v := range in {
			if i > 0 {
				q += "|"
			}

			q += "@" + field + ":"
			q += "[" + v[0] + " " + v[1] + "]"
		}
	}

	for field, in := range query.filtersTags {
		for _, v := range in {
			if q != "" {
				q += " "
			}

			q += "@" + field + ":{ " + strings.Join(v, " | ") + " }"
		}
	}

	for field, in := range query.filtersString {
		for _, v := range in {
			if q != "" {
				q += " "
			}

			q += "@" + field + ":( " + strings.Join(v, " | ") + " )"
		}
	}

	for field, in := range query.filtersNotNumeric {
		if q != "" {
			q += " "
		}

		for _, v := range in {
			q += "(@" + field + ":[-inf (" + v + "] | @" + field + ":[(" + v + " +inf])"
		}
	}

	for field, in := range query.filtersNotTags {
		for _, v := range in {
			if q != "" {
				q += " "
			}

			q += "-@" + field + ":{ " + strings.Join(v, " | ") + " }"
		}
	}

	for field, in := range query.filtersNotString {
		for _, v := range in {
			if q != "" {
				q += " "
			}

			q += "-@" + field + ":( " + strings.Join(v, " | ") + " )"
		}
	}

	if query.hasFakeDelete && !query.withFakeDelete {
		q += "-@FakeDelete:{true}"
	}

	if q == "" {
		q = "*"
	}

	args = append(args, q)

	for field, data := range query.filtersGeo {
		args = append(args, "GEOFILTER", field, data[0], data[1], data[2], data[3])
	}

	return args
}

func (r *RedisSearch) createIndexArgs(index *RedisSearchIndex, indexName string) []interface{} {
	indexName = r.redis.AddNamespacePrefix(indexName)

	if len(index.Prefixes) == 0 {
		panic(errors.New("missing redis search prefix"))
	}

	args := []interface{}{"FT.CREATE", indexName, "ON", "HASH", "PREFIX", len(index.Prefixes)}

	for _, prefix := range index.Prefixes {
		args = append(args, r.redis.AddNamespacePrefix(prefix))
	}

	if index.DefaultLanguage != "" {
		args = append(args, "LANGUAGE", index.DefaultLanguage)
	}

	if index.LanguageField != "" {
		args = append(args, "LANGUAGE_FIELD", index.LanguageField)
	}

	if index.DefaultScore > 0 {
		args = append(args, "SCORE", index.DefaultScore)
	}

	if index.ScoreField != "" {
		args = append(args, "SCORE_FIELD", index.ScoreField)
	}

	if index.MaxTextFields {
		args = append(args, "MAXTEXTFIELDS")
	}

	if index.NoOffsets {
		args = append(args, "NOOFFSETS")
	}

	if index.NoNHL {
		args = append(args, "NOHL")
	}

	if index.NoFields {
		args = append(args, "NOFIELDS")
	}

	if index.NoFreqs {
		args = append(args, "NOFREQS")
	}

	if index.SkipInitialScan {
		args = append(args, "SKIPINITIALSCAN")
	}

	if index.StopWords != nil {
		args = append(args, "STOPWORDS", len(index.StopWords))

		for _, word := range index.StopWords {
			args = append(args, word)
		}
	}

	args = append(args, "SCHEMA")

	for _, field := range index.Fields {
		fieldArgs := []interface{}{field.Name, field.Type}

		if field.Type == redisSearchIndexFieldText {
			if field.NoStem {
				fieldArgs = append(fieldArgs, "NOSTEM")
			}

			if field.Weight != 1 {
				fieldArgs = append(fieldArgs, "WEIGHT", field.Weight)
			}
		} else if field.Type == redisSearchIndexFieldTAG {
			if field.TagSeparator != "" && field.TagSeparator != ", " {
				fieldArgs = append(fieldArgs, "SEPARATOR", field.TagSeparator)
			}
		}

		if field.Sortable {
			fieldArgs = append(fieldArgs, "SORTABLE")
		}

		if field.NoIndex {
			fieldArgs = append(fieldArgs, "NOINDEX")
		}

		args = append(args, fieldArgs...)
	}

	return args
}

func (r *RedisSearch) createIndex(index *RedisSearchIndex) {
	args := r.createIndexArgs(index, index.Name)
	cmd := redis.NewStringCmd(r.ctx, args...)

	hasRedisLogger, redisLogger := r.engine.HasRedisLogger()

	start := getNow(hasRedisLogger)

	err := r.redis.Process(r.ctx, cmd)
	if hasRedisLogger {
		r.fillLogFields(redisLogger, "FT.CREATE", cmd.String(), start, err)
	}

	checkError(err)
}

func (r *RedisSearch) ListIndices() []string {
	cmd := redis.NewStringSliceCmd(r.ctx, "FT._LIST")

	hasRedisLogger, redisLogger := r.engine.HasRedisLogger()

	start := getNow(hasRedisLogger)

	err := r.redis.Process(r.ctx, cmd)

	if hasRedisLogger {
		r.fillLogFields(redisLogger, "FT.LIST", "FT.LIST", start, err)
	}

	checkError(err)

	res, err := cmd.Result()
	checkError(err)

	if r.redis.HasNamespace() {
		finalResult := make([]string, 0)
		prefix := r.redis.GetNamespace() + ":"

		for _, v := range res {
			if strings.HasPrefix(v, prefix) {
				finalResult = append(finalResult, r.redis.RemoveNamespacePrefix(v))
			}
		}

		return finalResult
	}

	return res
}

// nolint // info
func (r *RedisSearch) dropIndex(indexName string, withHashes bool) bool {
	indexName = r.redis.AddNamespacePrefix(indexName)
	args := []interface{}{"FT.DROPINDEX", indexName}
	if withHashes {
		args = append(args, "DD")
	}

	cmd := redis.NewStringCmd(r.ctx, args...)

	hasRedisLogger, redisLogger := r.engine.HasRedisLogger()

	start := getNow(hasRedisLogger)

	err := r.redis.Process(r.ctx, cmd)

	if hasRedisLogger {
		r.fillLogFields(redisLogger, "FT.DROPINDEX", cmd.String(), start, err)
	}

	if err != nil && strings.HasPrefix(err.Error(), "Unknown Index ") {
		return false
	}

	checkError(err)

	_, err = cmd.Result()
	checkError(err)

	return true
}

// nolint // info
func (r *RedisSearch) Info(indexName string) *RedisSearchIndexInfo {
	indexName = r.redis.AddNamespacePrefix(indexName)
	cmd := redis.NewSliceCmd(r.ctx, "FT.INFO", indexName)

	hasRedisLogger, redisLogger := r.engine.HasRedisLogger()

	start := getNow(hasRedisLogger)

	err := r.redis.Process(r.ctx, cmd)

	has := true
	if err != nil && err.Error() == "Unknown Index name" {
		err = nil
		has = false
	}

	if hasRedisLogger {
		r.fillLogFields(redisLogger, "FT.INFO", "FT.INFO "+indexName, start, err)
	}

	if !has {
		return nil
	}

	checkError(err)
	res, err := cmd.Result()

	checkError(err)
	info := &RedisSearchIndexInfo{}

	for i, row := range res {
		switch row {
		case "index_name":
			if r.redis.HasNamespace() {
				info.Name = r.redis.RemoveNamespacePrefix(res[i+1].(string))
			} else {
				info.Name = res[i+1].(string)
			}
		case "index_options":
			infoOptions := res[i+1].([]interface{})
			options := RedisSearchIndexInfoOptions{}
			for _, opt := range infoOptions {
				switch opt {
				case "NOFREQS":
					options.NoFreqs = true
				case "NOFIELDS":
					options.NoFields = true
				case "NOOFFSETS":
					options.NoOffsets = true
				case "MAXTEXTFIELDS":
					options.MaxTextFields = true
				}
			}
			info.Options = options
		case "index_definition":
			def := res[i+1].([]interface{})
			definition := RedisSearchIndexInfoDefinition{}
			for subKey, subValue := range def {
				switch subValue {
				case "key_type":
					definition.KeyType = def[subKey+1].(string)
				case "prefixes":
					prefixesRaw := def[subKey+1].([]interface{})
					prefixes := make([]string, len(prefixesRaw))
					for k, v := range prefixesRaw {
						prefixes[k] = v.(string)
					}
					definition.Prefixes = prefixes
				case "language_field":
					definition.LanguageField = def[subKey+1].(string)
				case "default_score":
					definition.DefaultScore = def[subKey+1].(float64)
				case "score_field":
					definition.ScoreField = def[subKey+1].(string)
				}
			}
			info.Definition = definition
		case "fields":
			fieldsRaw := res[i+1].([]interface{})
			fields := make([]RedisSearchIndexInfoField, len(fieldsRaw))
			for i, v := range fieldsRaw {
				def := v.([]interface{})
				field := RedisSearchIndexInfoField{Name: def[0].(string)}
				def = def[1:]
				for subKey, subValue := range def {
					switch subValue {
					case "type":
						field.Type = def[subKey+1].(string)
					case "WEIGHT":
						field.Weight = def[subKey+1].(float64)
					case "SORTABLE":
						field.Sortable = true
					case "NOSTEM":
						field.NoStem = true
					case "NOINDEX":
						field.NoIndex = true
					case "SEPARATOR":
						field.TagSeparator = def[subKey+1].(string)
					}
				}
				fields[i] = field
			}
			info.Fields = fields
		case "attributes":
			fieldsRaw := res[i+1].([]interface{})
			fields := make([]RedisSearchIndexInfoField, len(fieldsRaw))
			for i, v := range fieldsRaw {
				def := v.([]interface{})
				field := RedisSearchIndexInfoField{}
				for subKey, subValue := range def {
					switch subValue {
					case "identifier":
						field.Name = def[subKey+1].(string)
					case "type":
						field.Type = def[subKey+1].(string)
					case "WEIGHT":
						field.Weight = def[subKey+1].(float64)
					case "SORTABLE":
						field.Sortable = true
					case "NOSTEM":
						field.NoStem = true
					case "NOINDEX":
						field.NoIndex = true
					case "SEPARATOR":
						field.TagSeparator = def[subKey+1].(string)
					}
				}
				fields[i] = field
			}
			info.Fields = fields
		case "num_docs":
			if !math.IsNaN(res[i+1].(float64)) {
				info.NumDocs = uint64(res[i+1].(float64))
			}
		case "max_doc_id":
			if !math.IsNaN(res[i+1].(float64)) {
				info.MaxDocID = uint64(res[i+1].(float64))
			}
		case "num_terms":
			if !math.IsNaN(res[i+1].(float64)) {
				info.NumTerms = uint64(res[i+1].(float64))
			}
		case "num_records":
			if !math.IsNaN(res[i+1].(float64)) {
				info.NumRecords = uint64(res[i+1].(float64))
			}
		case "inverted_sz_mb":
			if !math.IsNaN(res[i+1].(float64)) {
				info.InvertedSzMB = res[i+1].(float64)
			}
		case "total_inverted_index_blocks":
			if !math.IsNaN(res[i+1].(float64)) {
				info.TotalInvertedIndexBlocks = res[i+1].(float64)
			}
		case "offset_vectors_sz_mb":
			if !math.IsNaN(res[i+1].(float64)) {
				info.OffsetVectorsSzMB = res[i+1].(float64)
			}
		case "doc_table_size_mb":
			if !math.IsNaN(res[i+1].(float64)) {
				info.DocTableSizeMB = res[i+1].(float64)
			}
		case "sortable_values_size_mb":
			if !math.IsNaN(res[i+1].(float64)) {
				info.SortableValuesSizeMB = res[i+1].(float64)
			}
		case "key_table_size_mb":
			if !math.IsNaN(res[i+1].(float64)) {
				info.KeyTableSizeMB = res[i+1].(float64)
			}
		case "records_per_doc_avg":
			if !math.IsNaN(res[i+1].(float64)) {
				info.RecordsPerDocAvg = int(res[i+1].(float64))
			}
		case "bytes_per_record_avg":
			if !math.IsNaN(res[i+1].(float64)) {
				info.BytesPerRecordAvg = int(res[i+1].(float64))
			}
		case "offsets_per_term_avg":
			if !math.IsNaN(res[i+1].(float64)) {
				info.OffsetsPerTermAvg = res[i+1].(float64)
			}
		case "offset_bits_per_record_avg":
			if !math.IsNaN(res[i+1].(float64)) {
				info.OffsetBitsPerRecordAvg = res[i+1].(float64)
			}
		case "hash_indexing_failures":
			if !math.IsNaN(res[i+1].(float64)) {
				info.HashIndexingFailures = uint64(res[i+1].(float64))
			}
		case "indexing":
			info.Indexing = res[i+1] == "1"
		case "percent_indexed":
			if !math.IsNaN(res[i+1].(float64)) {
				info.PercentIndexed = res[i+1].(float64)
			}
		case "stopwords_list":
			v := res[i+1].([]interface{})
			info.StopWords = make([]string, len(v))
			for i, v := range v {
				info.StopWords[i] = v.(string)
			}
		}
	}

	return info
}

func (r *RedisSearch) addAlter(index *RedisSearchIndex, documents uint64, changes []string) RedisSearchIndexAlter {
	query := fmt.Sprintf("%v", r.createIndexArgs(index, index.Name))[1:]
	query = query[0 : len(query)-1]
	alter := RedisSearchIndexAlter{Pool: r.redis.GetCode(), Name: index.Name, Query: query, Changes: changes, search: r}
	indexToAdd := index.Name
	alter.Execute = func() {
		alter.search.ForceReindex(indexToAdd)
	}
	alter.Documents = documents

	return alter
}

func (r *RedisSearch) fillLogFields(handlers []beeorm.LogHandler, operation, query string, start *time.Time, err error) {
	fillLogFields(r.engine, handlers, r.redis.GetCode(), "redis", operation, query, start, false, err)
}

func (r *RedisSearch) GetRedisSearchAlters() (alters []RedisSearchIndexAlter) {
	alters = make([]RedisSearchIndexAlter, 0)

	for _, pool := range r.engine.GetRegistry().GetRedisPools() {
		poolName := pool.GetCode()
		redisClient := r.engine.GetRedis(poolName)

		if redisClient.GetPoolConfig().GetDatabase() > 0 {
			continue
		}

		info := redisClient.Info("Modules")

		lines := strings.Split(info, "\r\n")
		hasModule := false
		var version *uint64

		for _, line := range lines {
			if strings.HasPrefix(line, "module:name=search") {
				for _, part := range strings.Split(line, ",") {
					if strings.HasPrefix(part, "ver=") {
						ver, err := strconv.ParseUint(part[4:7], 10, 64)
						checkError(err)

						version = &ver

						break
					}
				}

				hasModule = true

				break
			}
		}

		if !hasModule {
			continue
		}

		inRedis := make(map[string]bool)

		for _, name := range r.ListIndices() {
			def, has := r.redisSearchIndices[name]

			if !has {
				alter := RedisSearchIndexAlter{Pool: poolName, Query: "FT.DROPINDEX " + name, Name: name, search: r}
				nameToRemove := name
				alter.Execute = func() {
					alter.search.dropIndex(nameToRemove, false)
				}
				alter.Documents = r.Info(name).NumDocs
				alters = append(alters, alter)

				continue
			}

			inRedis[name] = true
			info := r.Info(name)
			changes := make([]string, 0)
			stopWords := def.StopWords

			bothEmpty := len(info.StopWords) == 0 && len(stopWords) == 0

			if !bothEmpty && !reflect.DeepEqual(info.StopWords, stopWords) {
				changes = append(changes, "different stop words")
			}

			prefixes := make([]string, 0)

			if len(def.Prefixes) == 0 || (len(def.Prefixes) == 1 && def.Prefixes[0] == "") {
				prefixes = append(prefixes, r.redis.AddNamespacePrefix(""))
				def.Prefixes = []string{""}
			} else {
				for _, v := range def.Prefixes {
					prefixes = append(prefixes, r.redis.AddNamespacePrefix(v))
				}
			}

			if !reflect.DeepEqual(info.Definition.Prefixes, prefixes) {
				changes = append(changes, "different prefixes")
			}

			languageField := def.LanguageField
			if languageField == "" && (version != nil && *version < 202) {
				languageField = "__language"
			}

			if info.Definition.LanguageField != languageField {
				changes = append(changes, "different language field")
			}

			scoreField := def.ScoreField
			if scoreField == "" && (version != nil && *version < 202) {
				scoreField = "__score"
			}

			if info.Definition.ScoreField != scoreField {
				changes = append(changes, "different score field")
			}

			if info.Options.NoFreqs != def.NoFreqs {
				changes = append(changes, "different option NOFREQS")
			}

			if info.Options.NoFields != def.NoFields {
				changes = append(changes, "different option NOFIELDS")
			}

			if info.Options.NoOffsets != def.NoOffsets {
				changes = append(changes, "different option NOOFFSETS")
			}

			if info.Options.MaxTextFields != def.MaxTextFields {
				changes = append(changes, "different option MAXTEXTFIELDS")
			}

			defaultScore := def.DefaultScore
			if defaultScore == 0 {
				defaultScore = 1
			}

			if info.Definition.DefaultScore != defaultScore {
				changes = append(changes, "different default score")
			}
		MAIN:
			for _, defField := range def.Fields {
				for _, infoField := range info.Fields {
					if defField.Name == infoField.Name {
						if defField.Type != infoField.Type {
							changes = append(changes, "different field type "+infoField.Name)
						} else {
							if defField.Type == redisSearchIndexFieldText {
								if defField.NoStem != infoField.NoStem {
									changes = append(changes, "different field nostem "+infoField.Name)
								}
								if defField.Weight != infoField.Weight {
									changes = append(changes, "different field weight "+infoField.Name)
								}
							} else if defField.Type == redisSearchIndexFieldTAG {
								if defField.TagSeparator != infoField.TagSeparator {
									changes = append(changes, "different field separator "+infoField.Name)
								}
							}
						}

						if defField.Sortable != infoField.Sortable {
							changes = append(changes, "different field sortable "+infoField.Name)
						}

						if defField.NoIndex != infoField.NoIndex {
							changes = append(changes, "different field noindex "+infoField.Name)
						}

						continue MAIN
					}
				}
				changes = append(changes, "new field "+defField.Name)
			}
		MAIN2:
			for _, infoField := range info.Fields {
				for _, defField := range def.Fields {
					if defField.Name == infoField.Name {
						continue MAIN2
					}
				}
				changes = append(changes, "unneeded field "+infoField.Name)
			}

			if len(changes) > 0 {
				alters = append(alters, r.addAlter(def, info.NumDocs, changes))
			}
		}

		for name, index := range r.redisSearchIndices {
			_, has := inRedis[name]
			if has {
				continue
			}

			alters = append(alters, r.addAlter(index, 0, []string{"new index"}))
		}
	}

	return alters
}

type RedisSearchIndexAlter struct {
	search    *RedisSearch
	Name      string
	Query     string
	Documents uint64
	Changes   []string
	Pool      string
	Execute   func()
}

type RedisSearchIndexInfoOptions struct {
	NoFreqs       bool
	NoOffsets     bool
	NoFields      bool
	MaxTextFields bool
}

type RedisSearchIndexInfo struct {
	Name                     string
	Options                  RedisSearchIndexInfoOptions
	Definition               RedisSearchIndexInfoDefinition
	Fields                   []RedisSearchIndexInfoField
	NumDocs                  uint64
	MaxDocID                 uint64
	NumTerms                 uint64
	NumRecords               uint64
	InvertedSzMB             float64
	TotalInvertedIndexBlocks float64
	OffsetVectorsSzMB        float64
	DocTableSizeMB           float64
	SortableValuesSizeMB     float64
	KeyTableSizeMB           float64
	RecordsPerDocAvg         int
	BytesPerRecordAvg        int
	OffsetsPerTermAvg        float64
	OffsetBitsPerRecordAvg   float64
	HashIndexingFailures     uint64
	Indexing                 bool
	PercentIndexed           float64
	StopWords                []string
}

type RedisSearchIndexInfoDefinition struct {
	KeyType       string
	Prefixes      []string
	LanguageField string
	ScoreField    string
	DefaultScore  float64
}

type RedisSearchIndexInfoField struct {
	Name         string
	Type         string
	Weight       float64
	Sortable     bool
	NoStem       bool
	NoIndex      bool
	TagSeparator string
}

type RedisSearchResult struct {
	Key          string
	Fields       []interface{}
	Score        float64
	ExplainScore []interface{}
}

func (r *RedisSearchResult) Value(field string) interface{} {
	for i := 0; i < len(r.Fields); i += 2 {
		if r.Fields[i] == field {
			val := r.Fields[i+1]
			asString := val.(string)

			if len(asString) == 1 {
				return redisSearchStringReplacerBackOne.Replace(asString)
			}

			return redisSearchStringReplacerBack.Replace(asString)
		}
	}

	return nil
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func getNow(has bool) *time.Time {
	if !has {
		return nil
	}

	s := time.Now()

	return &s
}

// nolint // info
func fillLogFields(engine beeorm.Engine, handlers []beeorm.LogHandler, pool, source, operation, query string, start *time.Time, cacheMiss bool, err error) {
	fields := map[string]interface{}{
		"operation": operation,
		"query":     query,
		"pool":      pool,
		"source":    source,
	}

	if cacheMiss {
		fields["miss"] = "TRUE"
	}

	meta := engine.GetMetaData()
	if len(meta) > 0 {
		fields["meta"] = meta
	}

	if start != nil {
		now := time.Now()
		fields["microseconds"] = time.Since(*start).Microseconds()
		fields["started"] = start.UnixNano()
		fields["finished"] = now.UnixNano()
	}

	if err != nil {
		fields["error"] = err.Error()
	}

	for _, handler := range handlers {
		handler.Handle(engine, fields)
	}
}
