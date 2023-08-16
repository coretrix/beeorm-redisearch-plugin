package redisearch

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/latolukasz/beeorm/v2"
)

type tableSchemaRedisSearch struct {
	index                   *RedisSearchIndex
	mapBindToRedisSearch    mapBindToRedisSearch
	mapBindToScanPointer    mapBindToScanPointer
	mapPointerToValue       mapPointerToValue
	columnMapping           map[string]int
	redisSearchPrefix       string
	redisSearchPrefixLen    int
	searchCacheName         string
	hasFakeDelete           bool
	hasSearchableFakeDelete bool
}

type mapBindToRedisSearch map[string]func(val interface{}) interface{}
type mapBindToScanPointer map[string]func() interface{}
type mapPointerToValue map[string]func(val interface{}) interface{}

func (tableSchema *tableSchemaRedisSearch) fillRedisSearchFromBind(redisSetter beeorm.RedisCacheSetter, bind beeorm.Bind, id uint64, insert bool) {
	delete(bind, "ID")

	values := make([]interface{}, 0)
	hasChangedField := false

	if tableSchema.hasFakeDelete {
		val, has := bind["FakeDelete"]

		if has && val != "0" {
			if !tableSchema.hasSearchableFakeDelete {
				redisSetter.Del(tableSchema.searchCacheName, tableSchema.redisSearchPrefix+strconv.FormatUint(id, 10))
			} else {
				values = append(values, "FakeDelete", "true")
				hasChangedField = true
			}
		}
	}

	idMap, has := tableSchema.mapBindToRedisSearch["ID"]

	if has {
		values = append(values, "ID", idMap(id))

		if !hasChangedField {
			hasChangedField = insert
		}
	}

	for k, f := range tableSchema.mapBindToRedisSearch {
		v, has := bind[k]
		if has {
			values = append(values, k, f(v))
			hasChangedField = true
		}
	}

	if hasChangedField {
		redisSetter.HSet(tableSchema.redisSearchPrefix+strconv.FormatUint(id, 10), values...)
	}
}

//nolint: funlen // info
func (tableSchema *tableSchemaRedisSearch) buildRedisSearchIndex(tableSchemaBeeORM beeorm.SettableEntitySchema, registry *beeorm.Registry) error {
	if len(tableSchema.index.Fields) <= 0 {
		return nil
	}

	tableSchema.searchCacheName = tableSchemaBeeORM.GetTag("ORM", "redisSearch", "default", "")
	if tableSchema.searchCacheName != "" {
		if !registry.HasRegisteredRedisPool(tableSchema.searchCacheName) {
			return fmt.Errorf("redis pool '%s' not found", tableSchema.searchCacheName)
		}
	} else {
		return fmt.Errorf("missing redis search pool tag in %s", tableSchemaBeeORM.GetEntityName())
	}

	hasSearchable := false

	for _, field := range tableSchema.index.Fields {
		if !field.NoIndex {
			hasSearchable = true

			break
		}
	}

	if !hasSearchable {
		tableSchema.index.Fields[0].NoIndex = false
	}

	tableSchema.index.StopWords = []string{}
	tableSchema.index.Name = tableSchemaBeeORM.GetEntityName()
	tableSchema.index.RedisPool = tableSchema.searchCacheName
	tableSchema.redisSearchPrefix = fmt.Sprintf("%x", sha256.Sum256([]byte(tableSchemaBeeORM.GetEntityName())))
	tableSchema.redisSearchPrefix = tableSchema.redisSearchPrefix[0:5] + ":"
	tableSchema.redisSearchPrefixLen = len(tableSchema.redisSearchPrefix)
	tableSchema.index.Prefixes = []string{tableSchema.redisSearchPrefix}
	tableSchema.index.NoOffsets = true
	tableSchema.index.NoFreqs = true
	tableSchema.index.NoNHL = true
	tableSchema.index.SkipInitialScan = true

	indexQuery := "SELECT `ID`"
	indexColumns := make([]string, 0)

	for column := range tableSchema.mapBindToRedisSearch {
		indexQuery += ",`" + column + "`"
		indexColumns = append(indexColumns, column)
	}

	indexQuery += " FROM `" + tableSchemaBeeORM.GetTableName() + "` WHERE `ID` > ?"
	if tableSchema.hasFakeDelete && !tableSchema.hasSearchableFakeDelete {
		indexQuery += " AND FakeDelete = 0"
	}

	indexQuery += " ORDER BY `ID` LIMIT " + strconv.Itoa(entityIndexerPage)

	tableSchema.index.Indexer = func(engine beeorm.Engine, lastID uint64, pusher RedisSearchIndexPusher) (newID uint64, hasMore bool) {
		results, def := engine.GetMysql(tableSchemaBeeORM.GetMysqlPool()).Query(indexQuery, lastID)
		defer def()

		total := 0
		pointers := make([]interface{}, len(indexColumns)+1)
		v := uint64(0)
		pointers[0] = &v

		for i, column := range indexColumns {
			pointers[i+1] = tableSchema.mapBindToScanPointer[column]()
		}

		for results.Next() {
			results.Scan(pointers...)

			lastID = *pointers[0].(*uint64)

			pusher.NewDocument(tableSchema.index.Prefixes[0] + strconv.FormatUint(lastID, 10))

			for i, column := range indexColumns {
				val := tableSchema.mapPointerToValue[column](pointers[i+1])
				pusher.setField(column, tableSchema.mapBindToRedisSearch[column](val))
			}

			pusher.PushDocument()

			total++
		}

		return lastID, total == entityIndexerPage
	}

	return nil
}

func buildUintField(tableSchema *tableSchemaRedisSearch, columnName, typeName string, hasSortable, hasSearchable bool) {
	tableSchema.index.AddNumericField(columnName, hasSortable, !hasSearchable)

	if hasSortable && typeName == "uint64" {
		tableSchema.mapBindToRedisSearch[columnName] = func(val interface{}) interface{} {
			valUint := uint64(0)

			if value, ok := val.(uint64); ok {
				valUint = value
			} else {
				var err error

				valUint, err = strconv.ParseUint(val.(string), 10, 64)

				if err != nil {
					panic(err)
				}
			}

			if valUint > math.MaxInt32 {
				panic(errors.New("integer too high for redis search sort field"))
			}

			return val
		}
	} else {
		tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapper
	}

	tableSchema.mapBindToScanPointer[columnName] = func() interface{} {
		v := uint64(0)

		return &v
	}

	tableSchema.mapPointerToValue[columnName] = func(val interface{}) interface{} {
		return *val.(*uint64)
	}
}

func buildUintPointerField(tableSchema *tableSchemaRedisSearch, columnName, typeName string, hasSortable, hasSearchable bool) {
	tableSchema.index.AddNumericField(columnName, hasSortable, !hasSearchable)

	if hasSortable && typeName == "*uint64" {
		tableSchema.mapBindToRedisSearch[columnName] = func(val interface{}) interface{} {
			if val == nil || val == "NULL" {
				return RedisSearchNullNumber
			}

			valUint := uint64(0)

			if value, ok := val.(uint64); ok {
				valUint = value
			} else {
				var err error

				valUint, err = strconv.ParseUint(val.(string), 10, 64)

				if err != nil {
					panic(err)
				}
			}

			if valUint > math.MaxInt32 {
				panic(errors.New("integer too high for redis search sort field"))
			}

			return val
		}
	} else {
		tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapperNullableNumeric
	}

	tableSchema.mapBindToScanPointer[columnName] = scanIntNullablePointer
	tableSchema.mapPointerToValue[columnName] = pointerUintNullableScan
}

func buildIntField(tableSchema *tableSchemaRedisSearch, columnName, typeName string, hasSortable, hasSearchable bool) {
	tableSchema.index.AddNumericField(columnName, hasSortable, !hasSearchable)

	if hasSortable && typeName == "int64" {
		tableSchema.mapBindToRedisSearch[columnName] = func(val interface{}) interface{} {
			valInt := int64(0)

			if value, ok := val.(int64); ok {
				valInt = value
			} else {
				var err error

				valInt, err = strconv.ParseInt(val.(string), 10, 64)

				if err != nil {
					panic(err)
				}
			}

			if valInt > math.MaxInt32 {
				panic(errors.New("integer too high for redis search sort field"))
			}

			return val
		}
	} else {
		tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapper
	}

	tableSchema.mapBindToScanPointer[columnName] = func() interface{} {
		v := int64(0)

		return &v
	}

	tableSchema.mapPointerToValue[columnName] = func(val interface{}) interface{} {
		return *val.(*int64)
	}
}

func buildIntPointerField(tableSchema *tableSchemaRedisSearch, columnName, typeName string, hasSortable, hasSearchable bool) {
	tableSchema.index.AddNumericField(columnName, hasSortable, !hasSearchable)

	if hasSortable && typeName == "*int64" {
		tableSchema.mapBindToRedisSearch[columnName] = func(val interface{}) interface{} {
			if val == nil || val == "NULL" {
				return RedisSearchNullNumber
			}

			valInt := int64(0)

			if value, ok := val.(int64); ok {
				valInt = value
			} else {
				var err error

				valInt, err = strconv.ParseInt(val.(string), 10, 64)

				if err != nil {
					panic(err)
				}
			}

			if valInt > math.MaxInt32 {
				panic(errors.New("integer too high for redis search sort field"))
			}

			return val
		}
	} else {
		tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapperNullableNumeric
	}

	tableSchema.mapBindToScanPointer[columnName] = scanIntNullablePointer
	tableSchema.mapPointerToValue[columnName] = pointerIntNullableScan
}

func buildStringField(tableSchema *tableSchemaRedisSearch, columnName string, hasSortable, hasSearchable, hasEnum bool, stem string, hasStem bool) {
	if hasEnum {
		tableSchema.index.AddTagField(columnName, hasSortable, !hasSearchable, ",")
		tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapperNullableString
	} else {
		tableSchema.index.AddTextField(columnName, 1.0, hasSortable, !hasSearchable, !hasStem || stem != "true")
		tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapperNullableString
	}

	tableSchema.mapBindToScanPointer[columnName] = func() interface{} {
		return &sql.NullString{}
	}

	tableSchema.mapPointerToValue[columnName] = func(val interface{}) interface{} {
		v := val.(*sql.NullString)
		if v.Valid {
			return v.String
		}

		return nil
	}
}

func buildStringPointerField(
	tableSchema *tableSchemaRedisSearch,
	columnName string,
	hasSortable bool,
	hasSearchable bool,
	hasEnum bool,
	stem string,
	hasStem bool,
) {
	if hasEnum {
		tableSchema.index.AddTagField(columnName, hasSortable, !hasSearchable, ",")
		tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapperNullableStringPointer
	} else {
		tableSchema.index.AddTextField(columnName, 1.0, hasSortable, !hasSearchable, !hasStem || stem != "true")
		tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapperNullableStringPointer
	}

	tableSchema.mapBindToScanPointer[columnName] = func() interface{} {
		return &sql.NullString{}
	}

	tableSchema.mapPointerToValue[columnName] = func(val interface{}) interface{} {
		v := val.(*sql.NullString)
		if v.Valid {
			return v.String
		}

		return nil
	}
}

func buildStringSliceField(tableSchema *tableSchemaRedisSearch, columnName string, hasSortable, hasSearchable bool) {
	tableSchema.index.AddTagField(columnName, hasSortable, !hasSearchable, ",")
	tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapperNullableString
	tableSchema.mapBindToScanPointer[columnName] = scanStringNullablePointer
	tableSchema.mapPointerToValue[columnName] = pointerStringNullableScan
}

func buildBoolField(tableSchema *tableSchemaRedisSearch, columnName string, hasSortable, hasSearchable bool) {
	if columnName == "FakeDelete" {
		tableSchema.index.AddTagField(columnName, hasSortable, !hasSearchable, ",")
		tableSchema.mapBindToRedisSearch[columnName] = func(val interface{}) interface{} {
			if value, ok := val.(uint64); ok {
				if value > 0 {
					return "true"
				}
			} else if value, ok := val.(string); ok {
				valUint, err := strconv.ParseUint(value, 10, 64)
				if err != nil {
					panic(err)
				}

				if valUint > 0 {
					return "true"
				}
			}

			return "false"
		}
		tableSchema.mapBindToScanPointer[columnName] = func() interface{} {
			v := uint64(0)

			return &v
		}
		tableSchema.mapPointerToValue[columnName] = func(val interface{}) interface{} {
			v := *val.(*uint64)

			return v
		}
	} else {
		tableSchema.index.AddTagField(columnName, hasSortable, !hasSearchable, ",")
		tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapperNullableBool
		tableSchema.mapBindToScanPointer[columnName] = scanBoolPointer
		tableSchema.mapPointerToValue[columnName] = pointerBoolScan
	}
}

func buildBoolPointerField(tableSchema *tableSchemaRedisSearch, columnName string, hasSortable, hasSearchable bool) {
	tableSchema.index.AddTagField(columnName, hasSortable, !hasSearchable, ",")
	tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapperNullableBool
	tableSchema.mapBindToScanPointer[columnName] = scanBoolNullablePointer
	tableSchema.mapPointerToValue[columnName] = pointerBoolNullableScan
}

func buildFloatField(tableSchema *tableSchemaRedisSearch, columnName string, hasSortable, hasSearchable bool) {
	tableSchema.index.AddNumericField(columnName, hasSortable, !hasSearchable)
	tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapper
	tableSchema.mapBindToScanPointer[columnName] = func() interface{} {
		v := float64(0)

		return &v
	}
	tableSchema.mapPointerToValue[columnName] = func(val interface{}) interface{} {
		return *val.(*float64)
	}
}

func buildFloatPointerField(tableSchema *tableSchemaRedisSearch, columnName string, hasSortable, hasSearchable bool) {
	tableSchema.index.AddNumericField(columnName, hasSortable, !hasSearchable)
	tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapperNullableNumeric
	tableSchema.mapBindToScanPointer[columnName] = scanFloatNullablePointer
	tableSchema.mapPointerToValue[columnName] = pointerFloatNullableScan
}

func buildTimePointerField(tableSchema *tableSchemaRedisSearch, columnName string, hasSortable, hasSearchable bool) {
	tableSchema.index.AddNumericField(columnName, hasSortable, !hasSearchable)
	tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapperNullableTime
	tableSchema.mapBindToScanPointer[columnName] = scanStringNullablePointer
	tableSchema.mapPointerToValue[columnName] = pointerStringNullableScan
}

func buildTimeField(tableSchema *tableSchemaRedisSearch, columnName string, hasSortable, hasSearchable bool) {
	tableSchema.index.AddNumericField(columnName, hasSortable, !hasSearchable)
	tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapperNullableTime
	tableSchema.mapBindToScanPointer[columnName] = scanStringPointer
	tableSchema.mapPointerToValue[columnName] = pointerStringScan
}

func buildPointerField(tableSchema *tableSchemaRedisSearch, columnName string, hasSortable, hasSearchable bool) {
	tableSchema.index.AddNumericField(columnName, hasSortable, !hasSearchable)
	tableSchema.mapBindToRedisSearch[columnName] = defaultRedisSearchMapperNullableNumeric
	tableSchema.mapBindToScanPointer[columnName] = scanIntNullablePointer
	tableSchema.mapPointerToValue[columnName] = pointerUintNullableScan
}

func buildPointersSliceField(tableSchema *tableSchemaRedisSearch, columnName string, _, hasSearchable bool) {
	if hasSearchable {
		tableSchema.index.AddTextField(columnName, 0, false, false, true)
		tableSchema.mapBindToRedisSearch[columnName] = func(val interface{}) interface{} {
			if val == nil || val == "" || val == "NULL" {
				return ""
			}

			holder := make([]map[string]interface{}, 0)

			if err := json.Unmarshal([]byte(val.(string)), &holder); err != nil {
				panic(err)
			}

			result := ""

			for i, v := range holder {
				if id, ok := v["ID"]; ok {
					result += "e" + strconv.FormatFloat(id.(float64), 'f', -1, 64)
				}

				if i != len(holder)-1 {
					result += " "
				}
			}

			return result
		}
		tableSchema.mapBindToScanPointer[columnName] = scanStringNullablePointer
		tableSchema.mapPointerToValue[columnName] = pointerStringNullableScan
	}
}

var defaultRedisSearchMapper = func(val interface{}) interface{} {
	return val
}

var defaultRedisSearchMapperNullableString = func(val interface{}) interface{} {
	if val == nil || val == "NULL" {
		return "NULL"
	}

	return EscapeRedisSearchString(val.(string))
}

var defaultRedisSearchMapperNullableStringPointer = func(val interface{}) interface{} {
	if val == nil || val == "NULL" {
		return "NULL"
	}

	return EscapeRedisSearchString(strings.TrimPrefix(strings.TrimSuffix(val.(string), `"`), `"`))
}

var defaultRedisSearchMapperNullableNumeric = func(val interface{}) interface{} {
	if val == nil || val == "NULL" {
		return RedisSearchNullNumber
	}

	return val
}

var defaultRedisSearchMapperNullableBool = func(val interface{}) interface{} {
	if val == nil || val == "NULL" {
		return "NULL"
	}

	if value, ok := val.(bool); ok && value {
		return "true"
	}

	if value, ok := val.(string); ok && (value == "true" || value == "1") {
		return "true"
	}

	return "false"
}

var defaultRedisSearchMapperNullableTime = func(val interface{}) interface{} {
	if val == nil || val == "NULL" {
		return RedisSearchNullNumber
	}

	v := val.(string)
	if v[0:10] == "0001-01-01" {
		return 0
	}

	if len(v) == 19 {
		t, _ := time.ParseInLocation(beeorm.TimeFormat, v, time.UTC)

		return t.Unix()
	}

	t, _ := time.ParseInLocation(beeorm.DateFormat, v, time.UTC)

	return t.Unix()
}

var pointerStringNullableScan = func(val interface{}) interface{} {
	v := val.(*sql.NullString)
	if v.Valid {
		return v.String
	}

	return nil
}

var scanStringNullablePointer = func() interface{} {
	return &sql.NullString{}
}

var scanIntNullablePointer = func() interface{} {
	return &sql.NullInt64{}
}

var pointerUintNullableScan = func(val interface{}) interface{} {
	v := val.(*sql.NullInt64)
	if v.Valid {
		return uint64(v.Int64)
	}

	return nil
}

var pointerIntNullableScan = func(val interface{}) interface{} {
	v := val.(*sql.NullInt64)
	if v.Valid {
		return v.Int64
	}

	return nil
}

var scanBoolPointer = func() interface{} {
	v := false

	return &v
}

var pointerBoolScan = func(val interface{}) interface{} {
	return *val.(*bool)
}

var scanBoolNullablePointer = func() interface{} {
	return &sql.NullBool{}
}

var pointerBoolNullableScan = func(val interface{}) interface{} {
	v := val.(*sql.NullBool)
	if v.Valid {
		return v.Bool
	}

	return nil
}

var scanFloatNullablePointer = func() interface{} {
	return &sql.NullFloat64{}
}

var pointerFloatNullableScan = func(val interface{}) interface{} {
	v := val.(*sql.NullFloat64)
	if v.Valid {
		return v.Float64
	}

	return nil
}

var scanStringPointer = func() interface{} {
	v := ""

	return &v
}

var pointerStringScan = func(val interface{}) interface{} {
	return *val.(*string)
}
