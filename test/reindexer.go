package test

import (
	"github.com/iliyanm/redisearch"
	"github.com/iliyanm/redisearch/test/customindex"
	"github.com/iliyanm/redisearch/test/entity"
	"github.com/latolukasz/beeorm/v2"
)

func reindexCustomIndexEntityOne(engine beeorm.Engine) {
	entities := make([]*entity.TestEntityOne, 0)
	engine.Search(beeorm.NewWhere("1 AND `FakeDelete` >= 0"), beeorm.NewPager(1, 1000), &entities)

	pusher := redisearch.NewRedisSearchIndexPusher(engine, "search_pool")
	customindex.SetEntityOneIndexFields(pusher, entities)
	pusher.Flush()
}
