package test

import (
	"github.com/latolukasz/beeorm/v2"

	redisearch "github.com/coretrix/beeorm-redisearch-plugin"
	"github.com/coretrix/beeorm-redisearch-plugin/test/customindex"
	"github.com/coretrix/beeorm-redisearch-plugin/test/entity"
)

func reindexCustomIndexEntityOne(engine beeorm.Engine) {
	entities := make([]*entity.TestEntityOne, 0)
	engine.Search(beeorm.NewWhere("1 AND `FakeDelete` >= 0"), beeorm.NewPager(1, 1000), &entities)

	pusher := redisearch.NewRedisSearchIndexPusher(engine, "search_pool")
	customindex.SetEntityOneIndexFields(pusher, entities)
	pusher.Flush()
}
