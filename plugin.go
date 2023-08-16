package redisearch

import (
	"reflect"
	"strconv"
	"sync"

	"github.com/latolukasz/beeorm/v2"
)

const (
	pluginCode        = "github.com/coretrix/hitrix/pkg/plugin/redisearch"
	optionsKey        = "redisearch_options"
	entityIndexerPage = 5000
)

type BeeormRedisearchPlugin struct {
	mu                    sync.Mutex
	pool                  string
	entitySchemaProcessed map[string]*struct{}
}

func Init(pool string) *BeeormRedisearchPlugin {
	return &BeeormRedisearchPlugin{pool: pool, entitySchemaProcessed: map[string]*struct{}{}}
}

func (p *BeeormRedisearchPlugin) RegisterCustomIndex(customIndex *RedisSearchIndex) {
	customIndices, ok := customIndicesInit[p.pool]
	if !ok {
		customIndices = append(customIndices, customIndex)
	}

	customIndicesInit[p.pool] = customIndices
}

func (p *BeeormRedisearchPlugin) GetCode() string {
	return pluginCode
}

//nolint //Function has too many statements
func (p *BeeormRedisearchPlugin) InterfaceInitEntitySchema(schema beeorm.SettableEntitySchema, registry *beeorm.Registry) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if _, has := p.entitySchemaProcessed[schema.GetEntityName()]; has {
		return nil
	}

	p.entitySchemaProcessed[schema.GetEntityName()] = &struct{}{}

	entityType := schema.GetType()

	redisSearchIndex := &tableSchemaRedisSearch{
		index:                nil,
		columnMapping:        map[string]int{},
		mapBindToRedisSearch: map[string]func(val interface{}) interface{}{},
		mapBindToScanPointer: map[string]func() interface{}{},
		mapPointerToValue:    map[string]func(val interface{}) interface{}{},
	}

	hsaFakeDelete := false
	hasSearchableFakeDelete := false

	for i, column := range schema.GetColumns() {
		redisSearchIndex.columnMapping[column] = i

		isSearchable := schema.GetTag(column, "searchable", "true", "") == "true"
		isSortable := schema.GetTag(column, "sortable", "true", "") == "true"
		hasEnum := schema.GetTag(column, "enum", "true", "") != ""
		stem := schema.GetTag(column, "stem", "true", "")
		hasStem := stem != ""

		if column == "FakeDelete" {
			hsaFakeDelete = true
			hasSearchableFakeDelete = isSearchable
		}

		if !isSearchable && !isSortable {
			continue
		}

		if redisSearchIndex.index == nil {
			redisSearchIndex.index = &RedisSearchIndex{}
		}

		structField, ok := entityType.FieldByName(column)
		if !ok {
			continue
		}

		kind := structField.Type.Kind()
		typeName := structField.Type.Name()

		isPointer := kind == reflect.Pointer
		isSlice := kind == reflect.Array || kind == reflect.Slice

		if isPointer {
			typeName = "*" + structField.Type.Elem().Name()
		}

		if isSlice {
			typeName = "[]" + structField.Type.Elem().Name()
		}

		switch typeName {
		case "uint",
			"uint8",
			"uint16",
			"uint32",
			"uint64":
			buildUintField(redisSearchIndex, column, typeName, isSortable, isSearchable)
		case "*uint",
			"*uint8",
			"*uint16",
			"*uint32",
			"*uint64":
			buildUintPointerField(redisSearchIndex, column, typeName, isSortable, isSearchable)
		case "int",
			"int8",
			"int16",
			"int32",
			"int64":
			buildIntField(redisSearchIndex, column, typeName, isSortable, isSearchable)
		case "*int",
			"*int8",
			"*int16",
			"*int32",
			"*int64":
			buildIntPointerField(redisSearchIndex, column, typeName, isSortable, isSearchable)
		case "string":
			buildStringField(redisSearchIndex, column, isSortable, isSearchable, hasEnum, stem, hasStem)
		case "*string":
			buildStringPointerField(redisSearchIndex, column, isSortable, isSearchable, hasEnum, stem, hasStem)
		case "[]string":
			buildStringSliceField(redisSearchIndex, column, isSortable, isSearchable)
		case "bool":
			buildBoolField(redisSearchIndex, column, isSortable, isSearchable)
		case "*bool":
			buildBoolPointerField(redisSearchIndex, column, isSortable, isSearchable)
		case "float32",
			"float64":
			buildFloatField(redisSearchIndex, column, isSortable, isSearchable)
		case "*float32",
			"*float64":
			buildFloatPointerField(redisSearchIndex, column, isSortable, isSearchable)
		case "*beeorm.CachedQuery":
			continue
		case "*Time":
			buildTimePointerField(redisSearchIndex, column, isSortable, isSearchable)
		case "Time":
			buildTimeField(redisSearchIndex, column, isSortable, isSearchable)
		default:
			if isPointer {
				buildPointerField(redisSearchIndex, column, isSortable, isSearchable)
			} else {
				buildPointersSliceField(redisSearchIndex, column, isSortable, isSearchable)
			}
		}
	}

	if redisSearchIndex.index == nil {
		return nil
	}

	redisSearchIndex.hasFakeDelete = hsaFakeDelete
	redisSearchIndex.hasSearchableFakeDelete = hasSearchableFakeDelete

	if err := redisSearchIndex.buildRedisSearchIndex(schema, registry); err != nil {
		return err
	}

	if indexes := redisSearchIndicesInit[redisSearchIndex.searchCacheName]; indexes == nil {
		redisSearchIndicesInit[redisSearchIndex.searchCacheName] = map[string]*RedisSearchIndex{}
	}

	redisSearchIndicesInit[redisSearchIndex.searchCacheName][redisSearchIndex.index.Name] = redisSearchIndex.index

	schema.SetPluginOption(pluginCode, optionsKey, redisSearchIndex)

	return nil
}

func (p *BeeormRedisearchPlugin) PluginInterfaceEntityFlushed(
	engine beeorm.Engine,
	event beeorm.EventEntityFlushed,
	setter beeorm.FlusherCacheSetter,
) {
	entitySchema := engine.GetRegistry().GetEntitySchema(event.EntityName())

	options := entitySchema.GetPluginOption(pluginCode, optionsKey)
	if options == nil {
		return
	}

	redisSearchSchema, ok := options.(*tableSchemaRedisSearch)
	if !ok {
		return
	}

	redisSetter := setter.GetRedisCacheSetter(redisSearchSchema.index.RedisPool)

	key := redisSearchSchema.redisSearchPrefix + strconv.FormatUint(event.EntityID(), 10)

	if event.Type() == beeorm.Delete {
		redisSetter.Del(redisSearchSchema.searchCacheName, key)

		return
	}

	redisSearchSchema.fillRedisSearchFromBind(redisSetter, event.After(), event.EntityID(), event.Type() == beeorm.Insert)
}
