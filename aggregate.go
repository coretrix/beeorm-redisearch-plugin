package redisearch

import "strconv"

type RedisSearchAggregation struct {
	query *RedisSearchQuery
	args  []interface{}
}

type RedisSearchAggregationSort struct {
	Field string
	Desc  bool
}

func (a *RedisSearchAggregation) GroupByField(field string, reduce ...AggregateReduce) *RedisSearchAggregation {
	return a.GroupByFields([]string{field}, reduce...)
}

func (a *RedisSearchAggregation) Sort(fields ...RedisSearchAggregationSort) *RedisSearchAggregation {
	a.args = append(a.args, "SORTBY", strconv.Itoa(len(fields)*2))

	for _, field := range fields {
		if field.Desc {
			a.args = append(a.args, field.Field, "DESC")
		} else {
			a.args = append(a.args, field.Field, "ASC")
		}
	}

	return a
}

func (a *RedisSearchAggregation) Load(fields *LoadFields) *RedisSearchAggregation {
	a.args = append(a.args, "LOAD", strconv.Itoa(len(fields.args)))

	for _, field := range fields.args {
		a.args = append(a.args, field)
	}

	return a
}

func (a *RedisSearchAggregation) LoadAll() *RedisSearchAggregation {
	a.args = append(a.args, "LOAD", "*")

	return a
}

func (a *RedisSearchAggregation) Apply(expression, alias string) *RedisSearchAggregation {
	a.args = append(a.args, "APPLY", expression, "AS", alias)

	return a
}

func (a *RedisSearchAggregation) Filter(expression string) *RedisSearchAggregation {
	a.args = append(a.args, "FILTER", expression)

	return a
}

func (a *RedisSearchAggregation) GroupByFields(fields []string, reduce ...AggregateReduce) *RedisSearchAggregation {
	a.args = append(a.args, "GROUPBY", len(fields))

	for _, f := range fields {
		a.args = append(a.args, f)
	}

	for _, r := range reduce {
		a.args = append(a.args, "REDUCE", r.function, len(r.args))
		a.args = append(a.args, r.args...)
		a.args = append(a.args, "AS", r.alias)
	}

	return a
}

type AggregateReduce struct {
	function string
	args     []interface{}
	alias    string
}

func NewAggregateReduceCount(alias string) AggregateReduce {
	return AggregateReduce{function: "COUNT", alias: alias}
}

func NewAggregateReduceCountDistinct(property, alias string, distinctish bool) AggregateReduce {
	f := "COUNT_DISTINCT"

	if distinctish {
		f += "ISH"
	}

	return AggregateReduce{function: f, args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceSum(property, alias string) AggregateReduce {
	return AggregateReduce{function: "SUM", args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceMin(property, alias string) AggregateReduce {
	return AggregateReduce{function: "MIN", args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceMax(property, alias string) AggregateReduce {
	return AggregateReduce{function: "MAX", args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceAvg(property, alias string) AggregateReduce {
	return AggregateReduce{function: "AVG", args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceStdDev(property, alias string) AggregateReduce {
	return AggregateReduce{function: "STDDEV", args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceQuantile(property, quantile, alias string) AggregateReduce {
	return AggregateReduce{function: "QUANTILE", args: []interface{}{property, quantile}, alias: alias}
}

func NewAggregateReduceToList(property, alias string) AggregateReduce {
	return AggregateReduce{function: "TOLIST", args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceFirstValue(property, alias string) AggregateReduce {
	return AggregateReduce{function: "FIRST_VALUE", args: []interface{}{property}, alias: alias}
}

func NewAggregateReduceFirstValueBy(property, byProperty, alias string, desc bool) AggregateReduce {
	sort := "ASC"

	if desc {
		sort = "DESC"
	}

	return AggregateReduce{function: "FIRST_VALUE", args: []interface{}{property, "BY", byProperty, sort}, alias: alias}
}

func NewAggregateReduceRandomSample(property, alias string, size ...int) AggregateReduce {
	sample := "1"

	if len(size) > 0 {
		sample = strconv.Itoa(size[0])
	}

	return AggregateReduce{function: "RANDOM_SAMPLE", args: []interface{}{property, sample}, alias: alias}
}

type LoadFields struct {
	args []string
}

func (lf *LoadFields) AddField(field string) {
	lf.args = append(lf.args, field)
}

func (lf *LoadFields) AddFieldWithAlias(field, alias string) {
	lf.args = append(lf.args, field, "AS", alias)
}
