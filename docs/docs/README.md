# Introduction

`beeorm-redisearch-plugin` is a plugin for [BeeORM](https://beeorm.io/guide/) which enables integration with Redisearch.

## What is Redisearch?
Redisearch is a source-available Redis module that enables querying, secondary indexing, and full-text search for Redis. These features enable multi-field queries, aggregation, exact phrase matching, numeric filtering, geo filtering and vector similarity semantic search on top of text queries.

## How it works?
Instead of making complex queries to MySQL, you can query the much faster Redisearch, which will return the entity ID and ORM will load the entity from Redis cache by this ID.
If the entity is not yet cached in Redis, the ORM will hit MySQL and then cache it.

## Installation

```
go get -u github.com/coretrix/beeorm-redisearch-plugin
``` 


## Configuration
1. You need to use [BeeORM](https://beeorm.io/guide/) in order to use this plugin.

2. Read how BeeORM plugins work and are set up [BeeORM Plugins](https://beeorm.io/plugins/).

3. You can configure the connection with orm yaml file or with Go code, the following example shows configuration with code.
```go
    beeormRegistry := beeorm.NewRegistry() // new BeeORM registry
    
    beeormRegistry.RegisterMySQLPool("root:root@tcp(localhost:11004)/redisearch?multiStatements=true", beeorm.MySQLPoolOptions{}, "default") // mysql connection
    
    beeormRegistry.RegisterRedis("localhost:11002", "", 1, "default") // pool needed for redis cache
    beeormRegistry.RegisterRedis("localhost:11002", "", 2, "streams_pool") // pool needed for redis streams, which are used for force reindex and indexing custom indexes
    beeormRegistry.RegisterRedis("localhost:11002", "", 0, "search_pool") // pool needed for redisearch itself, its important that it uses db 0!
    
    beeormRegistry.RegisterEntity(&entity.TestEntityOne{}) // redisearch enabled entities, see entities section
    beeormRegistry.RegisterEntity(&entity.TestEntityTwo{})

    rsPlugin := redisearch.Init("search_pool") // create plugin instance
    rsPlugin.RegisterCustomIndex(customindex.GetEntityOneIndex("search_pool")) // register custom indexes here, see custom indexes section
    
    beeormRegistry.RegisterPlugin(rsPlugin) // register redisearch plugin in BeeORM
    beeormRegistry.RegisterPlugin(fake_delete.Init(nil)) // register fake delete plugin in BeeORM (Redisearch plugin supports FakeDelete fields)

    beeormRegistry.RegisterRedisStream(redisearch.RedisSearchIndexerChannel, "streams_pool") // register the default redis stream that is needed for reindex of the indexes
    beeormRegistry.RegisterRedisStreamConsumerGroups(redisearch.RedisSearchIndexerChannel, redisearch.RedisSearchIndexerChannel+"_group")
	
    validatedRegistry, err := beeormRegistry.Validate() // validate the BeeORM registry
    if err != nil {
        panic(err)
    }
    
    beeormEngine = validatedRegistry.CreateEngine() // create BeeORM engine instance
    
    for _, alter := range beeormEngine.GetAlters() { // execute DB alters
        alter.Exec(beeormEngine)
    }

    redisSearch := redisearch.NewRedisSearch(ctx, beeormEngine, "search_pool") // create redisearch instance, use this instance to access all redisearch methods
    
    for _, alter := range redisSearch.GetRedisSearchAlters() { // execute redisearch alters
        alter.Execute()
    }
```
## Entities

In order to enable entities to be used in redisearch, you need to add the following tags:
- `redisSearch=search_pool` in beeorm.ORM field, it must correspond with your redisearch pool name
- `searchable` in each field that you would like to filter by later
- `sortable` in each filed that you would like to be able to sort by later

```go
    type TestEntityOne struct {
        beeorm.ORM    `orm:"table=test_entity_one;redisCache;redisSearch=search_pool"`
        ID            uint64           `orm:"searchable;sortable"`
        UintPtr       *uint64          `orm:"searchable;sortable"`
        Int           int64            `orm:"searchable;sortable"`
        IntPtr        *int64           `orm:"searchable;sortable"`
        Float         float64          `orm:"searchable;sortable"`
        FloatPtr      *float64         `orm:"searchable;sortable"`
        String        string           `orm:"searchable;sortable"`
        StringPtr     *string          `orm:"searchable;sortable"`
        StringEnum    string           `orm:"searchable;sortable;enum=entity.TestEntityEnumAll"`
        StringEnumPtr *string          `orm:"searchable;sortable;enum=entity.TestEntityEnumAll"`
        StringSlice   []string         `orm:"searchable"`
        Bool          bool             `orm:"searchable;sortable"`
        BoolPtr       *bool            `orm:"searchable;sortable"`
        Time          time.Time        `orm:"searchable;sortable;time=true"`
        TimePtr       *time.Time       `orm:"searchable;sortable;time=true"`
        ForeignKey    *TestEntityTwo   `orm:"searchable;sortable"`
        Many          []*TestEntityTwo `orm:"searchable"`
        FakeDelete    bool             `orm:"searchable"`
    }

    type TestEntityTwo struct {
        beeorm.ORM `orm:"table=test_entity_two;redisCache;redisSearch=search_pool"`
        ID         uint64
        Field      string `orm:"searchable;sortable"`
    }
```

## Filtering

I will show a few filtering examples. Please check `query.go` for all filtering methods.

#### RedisSearchOne

You must use `RedisSearchOne` only when you are sure the filters you provide will result in a unique row.

```go
	q := redisearch.NewRedisSearchQuery()
	q.FilterString("Name", "John")
    q.FilterUint("Age", 32)
	
	result := &entity.User{}
	redisSearch.RedisSearchOne(result, q, "Address") // loads the user inside the entity instance and loads its Address field reference
```

#### RedisSearchMany

```go
	q := redisearch.NewRedisSearchQuery()
	q.FilterString("Name", "John", "Mark") // you can provide multiple params as var args
    q.FilterUint("Age", 32, 33, 36, 40)
    q.FilterDateTimeMinMax(
        "CreatedAt",
        time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC),
        time.Date(2001, 5, 12, 2, 0, 0, 0, time.UTC),
    )
	q.Sort("Age", true) // sorts the results by Age desc
	
	results := make([]*entity.User, 0)
	redisSearch.RedisSearchMany(results, q,  beeorm.NewPager(1, 100)) // loads the users inside the entity slice instance
```

#### Raw queries

```go
	q := redisearch.NewRedisSearchQuery()
    q.FilterInt("Age", 2)
    q.AppendQueryRaw("@Name: Fran*") // in AppendQueryRaw you can put whatever query you want that satisfies the redisearch query language syntax
    q.Sort("Int", false)
    
    results := make([]*entity.TestEntityOne, 0)
    redisSearch.RedisSearchMany(&results, q, beeorm.NewPager(1, 100))
```

#### Aggregations

This plugin supports many aggregations, please see `aggregate.go` for all aggregation functions. The example below shows the `GroupByField` aggregation with reducer `NewAggregateReduceSum`.

```go
	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Age", 21, 22)

	a := q.Aggregate()
	a.GroupByField("@Age", redisearch.NewAggregateReduceSum("@WalletBalance", "total_amount"))
	a.Sort(redisearch.RedisSearchAggregationSort{
		Field: "@total_amount",
		Desc:  true,
	})

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

    for _, row := range result {
        if len(row) < 2 {
            continue
        }
        
        age, err := strconv.ParseUint(row["Age"], 10, 64)
        if err != nil {
            panic(err)
        }
        
        amount, err := strconv.ParseFloat(row["total_amount"], 64)
        if err != nil {
            panic(err)
        }
    }
```

## Custom indexes

Sometimes you may need to join MySQL tables in order to execute some complex query. Instead of doing this, you can simply create a custom index, which can contain fields from 1,2,3...100 tables.
In order to create custom index, first you must create a CRUD stream on the main entity the index will track. This CRUD stream will be used for indexing. [see BeeORM CRUD streams](https://beeorm.io/plugins/crud_stream.html)

## Custom index definition example

```go
    const UsersAddressesCustomIndex = "custom_index_users_addresses" // custom index name

    func GetUsersAddressesIndex(redisSearchPool string) *redisearch.RedisSearchIndex { // register this function in the plugin definition
        index := &redisearch.RedisSearchIndex{}
        index.Name = UsersAddressesCustomIndex
        index.RedisPool = redisSearchPool
        index.Prefixes = []string{UsersAddressesCustomIndex + ":"}
        
        // document fields
        index.AddNumericField("ID", true, false)
        index.AddTextField("Name", 1, true, false, false)
        index.AddTextField("AddressLine1", 1, true, false, false)
        
        // force reindex callback func, it will be called by the plugin each time you force reindex
        index.Indexer = usersAddressesIndexer
        
        return index
    }
    
    func SetUsersAddressesIndexFields(pusher redisearch.RedisSearchIndexPusher, entities []*entity.UserEntity) { // call this function in your CRUD stream consumer from UserEntity
        deletedIDs := make([]string, 0)
        
        for _, entityIter := range entities {
            id := UsersAddressesCustomIndex + ":" + strconv.FormatUint(entityIter.ID, 10)
            
            if entityIter.FakeDelete {
                deletedIDs = append(deletedIDs, id)
                
                continue
            }
        
            pusher.NewDocument(id)
            pusher.SetUint("ID", entityIter.ID)
            pusher.SetString("Name", entityIter.Name)
            pusher.SetString("AddressLine1", entityIter.Address.AddressLine1) // make sure you load the Address reference
            pusher.PushDocument()
        }
        
        if len(deletedIDs) != 0 {
            pusher.DeleteDocuments(deletedIDs...)
        }
    }
    
    func usersAddressesIndexer(engine beeorm.Engine, lastID uint64, pusher redisearch.RedisSearchIndexPusher) (newID uint64, hasMore bool) {
        where := beeorm.NewWhere("`ID` > ? AND `FakeDelete` >= 0 ORDER BY `ID` ASC", lastID)
        
        entities := make([]*entity.UserEntity, 0)
        engine.Search(where, beeorm.NewPager(1, 1000), &entities, "Address") // load the Address reference, because we use it in the custom index
        
        if len(entities) == 0 {
            return lastID, false
        }
        
        SetUsersAddressesIndexFields(pusher, entities)
        pusher.Flush()
        
        lastID = entities[len(entities)-1].ID
        
        return lastID, !(len(entities) < 1000)
    }
```

After you make the custom index definition, you need to register it in the plugin:

```go
    rsPlugin.RegisterCustomIndex(customindex.GetUsersAddressesIndex("search_pool"))
```

#### Custom index query

```go
	q := redisearch.NewRedisSearchQuery()
	q.FilterString("Name", "John")

	ids, total := redisearch.GetEntityIDs(redisSearch, customindex.EntityOneCustomIndex, q, beeorm.NewPager(1, 1000))

	result := &entity.TestEntityOne{}
	engine.LoadByID(ids[0], result)
```

## Statistics

Use `redisSearch.GetRedisSearchStatistics()` for many useful stats.

## Logging

Enable logging by `ormEngine.EnableQueryDebugCustom(false, true, false)`. It will output logs in the following format:

```
FT.SEARCH entity.TestEntityOne @String:( "test string 1" )-@FakeDelete:{true} NOCONTENT LIMIT 0 1: [1 2287b:1]
```
