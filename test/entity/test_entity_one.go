package entity

import (
	"time"

	"github.com/latolukasz/beeorm/v2"
)

const (
	TestEntityEnumOne = "one"
	TestEntityEnumTwo = "two"
)

type TestEntityEnum struct {
	TestEntityEnumOne string
	TestEntityEnumTwo string
}

var TestEntityEnumAll = TestEntityEnum{
	TestEntityEnumOne: TestEntityEnumOne,
	TestEntityEnumTwo: TestEntityEnumTwo,
}

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
