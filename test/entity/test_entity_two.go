package entity

import "github.com/latolukasz/beeorm/v2"

type TestEntityTwo struct {
	beeorm.ORM `orm:"table=test_entity_two;redisCache;redisSearch=search_pool"`
	ID         uint64
	Field      string `orm:"searchable;sortable"`
}
