package redisearch

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/latolukasz/beeorm/v2"
)

func (r *RedisSearch) RedisSearchAggregate(
	entity beeorm.Entity,
	query *RedisSearchAggregation,
	pager *beeorm.Pager,
) (result []map[string]string, totalRows uint64) {
	schema := r.engine.GetRegistry().GetEntitySchemaForEntity(entity)

	options := schema.GetPluginOption(pluginCode, optionsKey)
	if options == nil {
		panic(fmt.Errorf("entity %s is not searchable", schema.GetEntityName()))
	}

	redisSearchSchema, ok := options.(*tableSchemaRedisSearch)
	if !ok {
		panic(fmt.Errorf("entity %s is not searchable", schema.GetEntityName()))
	}

	if redisSearchSchema.index == nil {
		panic(fmt.Errorf("entity %s is not searchable", schema.GetEntityName()))
	}

	if query.query == nil {
		query.query = NewRedisSearchQuery()
	}

	if redisSearchSchema.hasSearchableFakeDelete {
		query.query.hasFakeDelete = true
	}

	return r.Aggregate(redisSearchSchema.index.Name, query, pager)
}

func (r *RedisSearch) RedisSearchIds(entity beeorm.Entity, query *RedisSearchQuery, pager *beeorm.Pager) (ids []uint64, totalRows uint64) {
	schema := r.engine.GetRegistry().GetEntitySchemaForEntity(entity)

	return redisSearchQuery(r, schema, query, pager)
}

func (r *RedisSearch) RedisSearch(
	query *RedisSearchQuery,
	pager *beeorm.Pager,
	entities interface{},
	references ...string,
) (totalRows uint64) {
	return redisSearchBase(r, entities, query, pager, references...)
}

func (r *RedisSearch) RedisSearchCount(entity beeorm.Entity, query *RedisSearchQuery) (totalRows uint64) {
	schema := r.engine.GetRegistry().GetEntitySchemaForEntity(entity)
	_, totalRows = redisSearchQuery(r, schema, query, beeorm.NewPager(0, 0))

	return totalRows
}

func redisSearchBase(redisSearch *RedisSearch,
	entities interface{},
	query *RedisSearchQuery,
	pager *beeorm.Pager,
	references ...string,
) (totalRows uint64) {
	elem := reflect.ValueOf(entities).Elem()

	_, has, name := getEntityTypeForSlice(redisSearch.engine.GetRegistry(), elem.Type(), true)
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}

	schema := redisSearch.engine.GetRegistry().GetEntitySchema(name)

	ids, total := redisSearchQuery(redisSearch, schema, query, pager)
	if total > 0 {
		redisSearch.engine.LoadByIDs(ids, entities, references...)
	}

	return total
}

func (r *RedisSearch) RedisSearchOne(entity beeorm.Entity, query *RedisSearchQuery, references ...string) (found bool) {
	return redisSearchOne(r, entity, query, references...)
}

func redisSearchOne(redisSearch *RedisSearch, entity beeorm.Entity, query *RedisSearchQuery, references ...string) (found bool) {
	schema := redisSearch.engine.GetRegistry().GetEntitySchemaForEntity(entity)

	ids, total := redisSearchQuery(redisSearch, schema, query, beeorm.NewPager(1, 1))
	if total == 0 {
		return false
	}

	found = redisSearch.engine.LoadByID(ids[0], entity, references...)

	return found
}

//nolint //cyclomatic complexity is high
func redisSearchQuery(redisSearch *RedisSearch,
	schema beeorm.EntitySchema,
	query *RedisSearchQuery,
	pager *beeorm.Pager,
) ([]uint64, uint64) {
	options := schema.GetPluginOption(pluginCode, optionsKey)
	if options == nil {
		panic(fmt.Errorf("entity %s is not searchable", schema.GetEntityName()))
	}

	redisSearchSchema, ok := options.(*tableSchemaRedisSearch)
	if !ok {
		panic(fmt.Errorf("entity %s is not searchable", schema.GetEntityName()))
	}

	if redisSearchSchema.index == nil {
		panic(fmt.Errorf("entity %s is not searchable", schema.GetEntityName()))
	}

	for k := range query.filtersString {
		_, has := redisSearchSchema.columnMapping[k]
		if !has {
			panic(fmt.Errorf("unknown field %s", k))
		}

		valid := false

	MAIN:
		for _, field := range redisSearchSchema.index.Fields {
			if field.Name == k {
				if field.Type == "TEXT" {
					valid = true

					break MAIN
				}

				panic(fmt.Errorf("string filter on fields %s with type %s not allowed", k, field.Type))
			}
		}

		if !valid {
			panic(fmt.Errorf("missing `searchable` tag for field %s", k))
		}
	}

	for k := range query.filtersNumeric {
		_, has := redisSearchSchema.columnMapping[k]
		if !has {
			panic(fmt.Errorf("unknown field %s", k))
		}

		valid := false
	MAIN2:
		for _, field := range redisSearchSchema.index.Fields {
			if field.Name == k {
				if field.Type == "NUMERIC" {
					valid = true

					break MAIN2
				}

				panic(fmt.Errorf("numeric filter on fields %s with type %s not allowed", k, field.Type))
			}
		}

		if !valid {
			panic(fmt.Errorf("missing `searchable` tag for field %s", k))
		}
	}

	for k := range query.filtersTags {
		_, has := redisSearchSchema.columnMapping[k]
		if !has {
			panic(fmt.Errorf("unknown field %s", k))
		}

		valid := false
	MAIN3:
		for _, field := range redisSearchSchema.index.Fields {
			if field.Name == k {
				if field.Type == "TAG" {
					valid = true

					break MAIN3
				}

				panic(fmt.Errorf("tag filter on fields %s with type %s not allowed", k, field.Type))
			}
		}

		if !valid {
			panic(fmt.Errorf("missing `searchable` tag for field %s", k))
		}
	}

	query.hasFakeDelete = redisSearchSchema.hasSearchableFakeDelete
	totalRows, res := redisSearch.search(redisSearchSchema.index.Name, query, pager, true)
	ids := make([]uint64, len(res))

	for i, v := range res {
		ids[i], _ = strconv.ParseUint(v.(string)[redisSearchSchema.redisSearchPrefixLen:], 10, 64)
	}

	return ids, totalRows
}

func NewRedisSearchQuery() *RedisSearchQuery {
	return &RedisSearchQuery{}
}

type RedisSearchQuery struct {
	query              string
	filtersNumeric     map[string][][]string
	filtersNotNumeric  map[string][]string
	filtersGeo         map[string][]interface{}
	filtersTags        map[string][][]string
	filtersNotTags     map[string][][]string
	filtersString      map[string][][]string
	filtersNotString   map[string][][]string
	inKeys             []interface{}
	inFields           []interface{}
	toReturn           []interface{}
	sortDesc           bool
	sortField          string
	verbatim           bool
	noStopWords        bool
	withScores         bool
	slop               int
	inOrder            bool
	lang               string
	explainScore       bool
	highlight          []interface{}
	highlightOpenTag   string
	highlightCloseTag  string
	summarize          []interface{}
	summarizeSeparator string
	summarizeFrags     int
	summarizeLen       int
	withFakeDelete     bool
	hasFakeDelete      bool
}

func (q *RedisSearchQuery) Query(query string) *RedisSearchQuery {
	q.query = EscapeRedisSearchString(query)

	return q
}

func (q *RedisSearchQuery) WithFakeDeleteRows() *RedisSearchQuery {
	q.withFakeDelete = true

	return q
}

func (q *RedisSearchQuery) QueryRaw(query string) *RedisSearchQuery {
	q.query = query

	return q
}

func (q *RedisSearchQuery) AppendQueryRaw(query string) *RedisSearchQuery {
	q.query += query

	return q
}

func (q *RedisSearchQuery) filterNumericMinMax(field string, min, max string) *RedisSearchQuery {
	if q.filtersNumeric == nil {
		q.filtersNumeric = make(map[string][][]string)
	}

	q.filtersNumeric[field] = append(q.filtersNumeric[field], []string{min, max})

	return q
}

func (q *RedisSearchQuery) filterNotNumeric(field string, val string) *RedisSearchQuery {
	if q.filtersNotNumeric == nil {
		q.filtersNotNumeric = make(map[string][]string)
	}

	q.filtersNotNumeric[field] = append(q.filtersNotNumeric[field], val)

	return q
}

func (q *RedisSearchQuery) FilterIntMinMax(field string, min, max int64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatInt(min, 10), strconv.FormatInt(max, 10))
}

func (q *RedisSearchQuery) FilterInt(field string, value ...int64) *RedisSearchQuery {
	for _, val := range value {
		q.FilterIntMinMax(field, val, val)
	}

	return q
}

func (q *RedisSearchQuery) FilterNotInt(field string, value ...int64) *RedisSearchQuery {
	for _, val := range value {
		q.filterNotNumeric(field, strconv.FormatInt(val, 10))
	}

	return q
}

func (q *RedisSearchQuery) FilterIntNull(field string) *RedisSearchQuery {
	return q.FilterInt(field, RedisSearchNullNumber)
}

func (q *RedisSearchQuery) FilterNotIntNull(field string) *RedisSearchQuery {
	return q.FilterNotInt(field, RedisSearchNullNumber)
}

func (q *RedisSearchQuery) FilterIntGreaterEqual(field string, value int64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatInt(value, 10), "+inf")
}

func (q *RedisSearchQuery) FilterIntGreater(field string, value int64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "("+strconv.FormatInt(value, 10), "+inf")
}

func (q *RedisSearchQuery) FilterIntLessEqual(field string, value int64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", strconv.FormatInt(value, 10))
}

func (q *RedisSearchQuery) FilterIntLess(field string, value int64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", "("+strconv.FormatInt(value, 10))
}

func (q *RedisSearchQuery) FilterUintMinMax(field string, min, max uint64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatUint(min, 10), strconv.FormatUint(max, 10))
}

func (q *RedisSearchQuery) FilterUint(field string, value ...uint64) *RedisSearchQuery {
	for _, val := range value {
		q.FilterUintMinMax(field, val, val)
	}

	return q
}

func (q *RedisSearchQuery) FilterUintNull(field string) *RedisSearchQuery {
	return q.FilterInt(field, RedisSearchNullNumber)
}

func (q *RedisSearchQuery) FilterUintGreaterEqual(field string, value uint64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatUint(value, 10), "+inf")
}

func (q *RedisSearchQuery) FilterUintGreater(field string, value uint64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "("+strconv.FormatUint(value, 10), "+inf")
}

func (q *RedisSearchQuery) FilterUintLessEqual(field string, value uint64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", strconv.FormatUint(value, 10))
}

func (q *RedisSearchQuery) FilterUintLess(field string, value uint64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", "("+strconv.FormatUint(value, 10))
}

func (q *RedisSearchQuery) FilterString(field string, value ...string) *RedisSearchQuery {
	return q.filterString(field, true, false, false, value...)
}

func (q *RedisSearchQuery) FilterNotString(field string, value ...string) *RedisSearchQuery {
	return q.filterString(field, true, true, false, value...)
}

func (q *RedisSearchQuery) FilterManyReferenceIn(field string, id ...uint64) *RedisSearchQuery {
	return q.filterString(field, false, false, false, q.buildRefMAnyValues(id)...)
}

func (q *RedisSearchQuery) FilterManyReferenceNotIn(field string, id ...uint64) *RedisSearchQuery {
	return q.filterString(field, false, true, false, q.buildRefMAnyValues(id)...)
}

func (q *RedisSearchQuery) QueryField(field string, value ...string) *RedisSearchQuery {
	return q.filterString(field, false, false, false, value...)
}

func (q *RedisSearchQuery) QueryFieldPrefixMatch(field string, value ...string) *RedisSearchQuery {
	return q.filterString(field, true, false, true, value...)
}

func (q *RedisSearchQuery) buildRefMAnyValues(id []uint64) []string {
	values := make([]string, len(id))
	for i, k := range id {
		values[i] = "e" + strconv.FormatUint(k, 10)
	}

	return values
}

func (q *RedisSearchQuery) filterString(field string, exactPhrase, not, starts bool, value ...string) *RedisSearchQuery {
	if len(value) == 0 {
		return q
	}

	if not {
		if q.filtersNotString == nil {
			q.filtersNotString = make(map[string][][]string)
		}
	} else {
		if q.filtersString == nil {
			q.filtersString = make(map[string][][]string)
		}
	}

	valueEscaped := make([]string, len(value))

	for i, v := range value {
		if v == "" {
			valueEscaped[i] = "\"NULL\""
		} else {
			if starts {
				values := strings.Split(strings.Trim(v, " "), " ")
				escaped := ""
				k := 0

				for _, val := range values {
					if len(val) >= 2 {
						if k > 0 {
							escaped += " "
						}
						escaped += EscapeRedisSearchString(val) + "*"
						k++
					}
				}

				if k == 0 {
					panic(fmt.Errorf("search start with requires min one word with 2 characters"))
				}

				valueEscaped[i] = escaped
			} else if exactPhrase {
				valueEscaped[i] = "\"" + EscapeRedisSearchString(v) + "\""
			} else {
				valueEscaped[i] = EscapeRedisSearchString(v)
			}
		}
	}

	if not {
		q.filtersNotString[field] = append(q.filtersNotString[field], valueEscaped)
	} else {
		q.filtersString[field] = append(q.filtersString[field], valueEscaped)
	}

	return q
}

func (q *RedisSearchQuery) FilterFloatMinMax(field string, min, max float64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatFloat(min-0.00001, 'f', -1, 64),
		strconv.FormatFloat(max+0.00001, 'f', -1, 64))
}

func (q *RedisSearchQuery) FilterFloat(field string, value ...float64) *RedisSearchQuery {
	for _, val := range value {
		q.FilterFloatMinMax(field, val, val)
	}

	return q
}

func (q *RedisSearchQuery) FilterFloatGreaterEqual(field string, value float64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatFloat(value-0.00001, 'f', -1, 64), "+inf")
}

func (q *RedisSearchQuery) FilterFloatGreater(field string, value float64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "("+strconv.FormatFloat(value+0.00001, 'f', -1, 64), "+inf")
}

func (q *RedisSearchQuery) FilterFloatLessEqual(field string, value float64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", strconv.FormatFloat(value+0.00001, 'f', -1, 64))
}

func (q *RedisSearchQuery) FilterFloatLess(field string, value float64) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", "("+strconv.FormatFloat(value-0.00001, 'f', -1, 64))
}

func (q *RedisSearchQuery) FilterFloatNull(field string) *RedisSearchQuery {
	return q.FilterFloat(field, RedisSearchNullNumber)
}

func (q *RedisSearchQuery) FilterDateMinMax(field string, min, max time.Time) *RedisSearchQuery {
	return q.FilterIntMinMax(field, q.cutDate(min), q.cutDate(max))
}

func (q *RedisSearchQuery) FilterDate(field string, date time.Time) *RedisSearchQuery {
	unix := q.cutDate(date)

	return q.FilterIntMinMax(field, unix, unix)
}

func (q *RedisSearchQuery) FilterNotDate(field string, date time.Time) *RedisSearchQuery {
	return q.filterNotNumeric(field, strconv.FormatInt(q.cutDate(date), 10))
}

func (q *RedisSearchQuery) FilterDateNull(field string) *RedisSearchQuery {
	return q.FilterInt(field, RedisSearchNullNumber)
}

func (q *RedisSearchQuery) FilterNotDateNull(field string) *RedisSearchQuery {
	return q.FilterNotInt(field, RedisSearchNullNumber)
}

func (q *RedisSearchQuery) FilterDateGreaterEqual(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatInt(q.cutDate(date), 10), "+inf")
}

func (q *RedisSearchQuery) FilterDateGreater(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "("+strconv.FormatInt(q.cutDate(date), 10), "+inf")
}

func (q *RedisSearchQuery) FilterDateLessEqual(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", strconv.FormatInt(q.cutDate(date), 10))
}

func (q *RedisSearchQuery) FilterDateLess(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", "("+strconv.FormatInt(q.cutDate(date), 10))
}

func (q *RedisSearchQuery) FilterDateTimeMinMax(field string, min, max time.Time) *RedisSearchQuery {
	return q.FilterIntMinMax(field, q.cutDateTime(min), q.cutDateTime(max))
}

func (q *RedisSearchQuery) FilterDateTime(field string, date time.Time) *RedisSearchQuery {
	unix := q.cutDateTime(date)

	return q.FilterIntMinMax(field, unix, unix)
}

func (q *RedisSearchQuery) FilterDateTimeNull(field string) *RedisSearchQuery {
	return q.FilterInt(field, RedisSearchNullNumber)
}

func (q *RedisSearchQuery) FilterDateTimeGreaterEqual(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, strconv.FormatInt(q.cutDateTime(date), 10), "+inf")
}

func (q *RedisSearchQuery) FilterDateTimeGreater(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "("+strconv.FormatInt(q.cutDateTime(date), 10), "+inf")
}

func (q *RedisSearchQuery) FilterDateTimeLessEqual(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", strconv.FormatInt(q.cutDateTime(date), 10))
}

func (q *RedisSearchQuery) FilterDateTimeLess(field string, date time.Time) *RedisSearchQuery {
	return q.filterNumericMinMax(field, "-inf", "("+strconv.FormatInt(q.cutDateTime(date), 10))
}

func (q *RedisSearchQuery) FilterTag(field string, tag ...string) *RedisSearchQuery {
	if q.filtersTags == nil {
		q.filtersTags = make(map[string][][]string)
	}

	tagEscaped := make([]string, len(tag))

	for i, v := range tag {
		if v == "" {
			v = "NULL"
		} else {
			v = EscapeRedisSearchString(v)
		}

		tagEscaped[i] = v
	}

	q.filtersTags[field] = append(q.filtersTags[field], tagEscaped)

	return q
}

func (q *RedisSearchQuery) FilterNotTag(field string, tag ...string) *RedisSearchQuery {
	if q.filtersNotTags == nil {
		q.filtersNotTags = make(map[string][][]string)
	}

	tagEscaped := make([]string, len(tag))

	for i, v := range tag {
		if v == "" {
			v = "NULL"
		} else {
			v = EscapeRedisSearchString(v)
		}

		tagEscaped[i] = v
	}

	q.filtersNotTags[field] = append(q.filtersNotTags[field], tagEscaped)

	return q
}

func (q *RedisSearchQuery) FilterBool(field string, value bool) *RedisSearchQuery {
	if value {
		return q.FilterTag(field, "true")
	}

	return q.FilterTag(field, "false")
}

func (q *RedisSearchQuery) FilterGeo(field string, lon, lat, radius float64, unit string) *RedisSearchQuery {
	if q.filtersGeo == nil {
		q.filtersGeo = make(map[string][]interface{})
	}

	q.filtersGeo[field] = []interface{}{lon, lat, radius, unit}

	return q
}

func (q *RedisSearchQuery) Sort(field string, desc bool) *RedisSearchQuery {
	q.sortField = field
	q.sortDesc = desc

	return q
}

func (q *RedisSearchQuery) Aggregate() *RedisSearchAggregation {
	return &RedisSearchAggregation{query: q}
}

func (q *RedisSearchQuery) Verbatim() *RedisSearchQuery {
	q.verbatim = true

	return q
}

func (q *RedisSearchQuery) NoStopWords() *RedisSearchQuery {
	q.noStopWords = true

	return q
}

func (q *RedisSearchQuery) WithScores() *RedisSearchQuery {
	q.withScores = true

	return q
}

func (q *RedisSearchQuery) InKeys(key ...string) *RedisSearchQuery {
	for _, k := range key {
		q.inKeys = append(q.inKeys, k)
	}

	return q
}

func (q *RedisSearchQuery) InFields(field ...string) *RedisSearchQuery {
	for _, k := range field {
		q.inFields = append(q.inFields, k)
	}

	return q
}

func (q *RedisSearchQuery) Return(field ...string) *RedisSearchQuery {
	for _, k := range field {
		q.toReturn = append(q.toReturn, k)
	}

	return q
}

func (q *RedisSearchQuery) Slop(slop int) *RedisSearchQuery {
	q.slop = slop
	if q.slop == 0 {
		q.slop = -1
	}

	return q
}

func (q *RedisSearchQuery) InOrder() *RedisSearchQuery {
	q.inOrder = true

	return q
}

func (q *RedisSearchQuery) ExplainScore() *RedisSearchQuery {
	q.explainScore = true

	return q
}

func (q *RedisSearchQuery) Lang(lang string) *RedisSearchQuery {
	q.lang = lang

	return q
}

func (q *RedisSearchQuery) Highlight(field ...string) *RedisSearchQuery {
	if q.highlight == nil {
		q.highlight = make([]interface{}, 0)
	}

	for _, k := range field {
		q.highlight = append(q.highlight, k)
	}

	return q
}

func (q *RedisSearchQuery) HighlightTags(openTag, closeTag string) *RedisSearchQuery {
	q.highlightOpenTag = openTag
	q.highlightCloseTag = closeTag

	return q
}

func (q *RedisSearchQuery) Summarize(field ...string) *RedisSearchQuery {
	if q.summarize == nil {
		q.summarize = make([]interface{}, 0)
	}

	for _, k := range field {
		q.summarize = append(q.summarize, k)
	}

	return q
}

func (q *RedisSearchQuery) SummarizeOptions(separator string, frags, len int) *RedisSearchQuery {
	q.summarizeSeparator = separator
	q.summarizeFrags = frags
	q.summarizeLen = len

	return q
}

func (q *RedisSearchQuery) cutDate(date time.Time) int64 {
	return time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC).Unix()
}

func (q *RedisSearchQuery) cutDateTime(date time.Time) int64 {
	return time.Date(date.Year(), date.Month(), date.Day(), date.Hour(), date.Minute(), date.Second(), 0, time.UTC).Unix()
}

func getEntityTypeForSlice(registry beeorm.ValidatedRegistry, sliceType reflect.Type, checkIsSlice bool) (reflect.Type, bool, string) {
	name := sliceType.String()
	if name[0] == 42 {
		name = name[1:]
	}

	if name[0] == 91 {
		name = name[3:]
	} else if checkIsSlice {
		panic(fmt.Errorf("interface %s is no slice of beeorm.Entity", sliceType.String()))
	}

	schema := registry.GetEntitySchema(name)

	return schema.GetType(), true, name
}
