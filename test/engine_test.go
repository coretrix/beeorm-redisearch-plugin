package test

import (
	"context"
	"testing"

	"github.com/latolukasz/beeorm/v2"
	"github.com/latolukasz/beeorm/v2/plugins/fake_delete"

	redisearch "github.com/coretrix/beeorm-redisearch-plugin"
	"github.com/coretrix/beeorm-redisearch-plugin/test/customindex"
	"github.com/coretrix/beeorm-redisearch-plugin/test/entity"
)

var beeormEngine beeorm.Engine

func TestMain(m *testing.M) {
	m.Run()
	beeormEngine.GetRedis("streams_pool").FlushDB()
}

func createTestEngine(ctx context.Context) (beeorm.Engine, *redisearch.RedisSearch) {
	if beeormEngine == nil {
		beeormRegistry := beeorm.NewRegistry()

		beeormRegistry.RegisterMySQLPool("root:root@tcp(localhost:13306)/redisearch?multiStatements=true", beeorm.MySQLPoolOptions{}, "default")

		beeormRegistry.RegisterRedis("localhost:16379", "", 1, "default")
		beeormRegistry.RegisterRedis("localhost:16379", "", 2, "streams_pool")
		beeormRegistry.RegisterRedis("localhost:16379", "", 0, "search_pool")

		beeormRegistry.RegisterEntity(&entity.TestEntityOne{})
		beeormRegistry.RegisterEntity(&entity.TestEntityTwo{})

		beeormRegistry.RegisterEnumStruct("entity.TestEntityEnumAll", entity.TestEntityEnumAll)

		rsPlugin := redisearch.Init("search_pool")
		rsPlugin.RegisterCustomIndex(customindex.GetEntityOneIndex("search_pool"))

		beeormRegistry.RegisterPlugin(rsPlugin)
		beeormRegistry.RegisterPlugin(fake_delete.Init(nil))

		beeormRegistry.RegisterRedisStream(redisearch.RedisSearchIndexerChannel, "streams_pool")
		beeormRegistry.RegisterRedisStreamConsumerGroups(redisearch.RedisSearchIndexerChannel, redisearch.RedisSearchIndexerChannel+"_group")

		validatedRegistry, err := beeormRegistry.Validate()
		if err != nil {
			panic(err)
		}

		beeormEngine = validatedRegistry.CreateEngine()

		for _, alter := range beeormEngine.GetAlters() {
			alter.Exec(beeormEngine)
		}
	}

	truncateTables(beeormEngine.GetMysql())

	beeormEngine.GetRedis().FlushDB()
	beeormEngine.GetRedis("search_pool").FlushDB()

	redisSearch := redisearch.NewRedisSearch(ctx, beeormEngine, "search_pool")

	for _, alter := range redisSearch.GetRedisSearchAlters() {
		alter.Execute()
	}

	return beeormEngine, redisSearch
}

func truncateTables(dbService *beeorm.DB) {
	var query string
	rows, deferF := dbService.Query(
		"SELECT CONCAT('delete from  ',table_schema,'.',table_name,';' , 'ALTER TABLE ', table_schema,'.',table_name , ' AUTO_INCREMENT = 1;') AS query " +
			"FROM information_schema.tables WHERE table_schema IN ('" + dbService.GetPoolConfig().GetDatabase() + "');",
	)

	defer deferF()

	if rows != nil {
		var queries string

		for rows.Next() {
			rows.Scan(&query)
			queries += query
		}

		_, def := dbService.Query("SET FOREIGN_KEY_CHECKS=0;" + queries + "SET FOREIGN_KEY_CHECKS=1")
		defer def()
	}
}
