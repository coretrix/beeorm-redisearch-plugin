package test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/latolukasz/beeorm/v2"
	"github.com/stretchr/testify/assert"
	"github.com/xorcare/pointer"

	redisearch "github.com/coretrix/beeorm-redisearch-plugin"
	"github.com/coretrix/beeorm-redisearch-plugin/test/customindex"
	"github.com/coretrix/beeorm-redisearch-plugin/test/entity"
)

func TestQueryField(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	testEntityFK := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	engine.Flush(testEntityFK)

	engine.Flush(&entity.TestEntityOne{
		String:     "test string 1",
		ForeignKey: testEntityFK,
	})

	q := redisearch.NewRedisSearchQuery()
	q.QueryField("String", "test string 1")

	result := &entity.TestEntityOne{}
	assert.True(t, redisSearch.RedisSearchOne(result, q, "ForeignKey"))

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	result = &entity.TestEntityOne{}
	assert.True(t, redisSearch.RedisSearchOne(result, q, "ForeignKey"))
}

func TestQueryFieldPrefixMatch(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	testEntityFK := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	engine.Flush(testEntityFK)

	engine.Flush(&entity.TestEntityOne{
		String:     "test string 1",
		ForeignKey: testEntityFK,
	})

	q := redisearch.NewRedisSearchQuery()
	q.QueryFieldPrefixMatch("String", "test string 1")

	result := &entity.TestEntityOne{}
	assert.True(t, redisSearch.RedisSearchOne(result, q, "ForeignKey"))

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	result = &entity.TestEntityOne{}
	assert.True(t, redisSearch.RedisSearchOne(result, q, "ForeignKey"))
}

func TestFilterString(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	testEntityFK := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	engine.Flush(testEntityFK)

	engine.Flush(&entity.TestEntityOne{
		String:     "test string 1",
		ForeignKey: testEntityFK,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterString("String", "test string 1")

	result := &entity.TestEntityOne{}
	assert.True(t, redisSearch.RedisSearchOne(result, q, "ForeignKey"))

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	result = &entity.TestEntityOne{}
	assert.True(t, redisSearch.RedisSearchOne(result, q, "ForeignKey"))
}

func TestFilterNotString(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	testEntityFK := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	engine.Flush(testEntityFK)

	engine.Flush(&entity.TestEntityOne{
		String:     "test string 1",
		ForeignKey: testEntityFK,
	})
	engine.Flush(&entity.TestEntityOne{
		String:     "test string 2",
		ForeignKey: testEntityFK,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterNotString("String", "test string 1")

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
}

func TestFilterStringPtr(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	testEntityFK := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	engine.Flush(testEntityFK)

	engine.Flush(&entity.TestEntityOne{
		StringPtr:  pointer.String("test string 2"),
		ForeignKey: testEntityFK,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterString("StringPtr", "test string 2")

	result := &entity.TestEntityOne{}
	assert.True(t, redisSearch.RedisSearchOne(result, q, "ForeignKey"))

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	result = &entity.TestEntityOne{}
	assert.True(t, redisSearch.RedisSearchOne(result, q, "ForeignKey"))
}

func TestFilterInt(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int: 2,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 4,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 5,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 7,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterInt("Int", 3, 4, 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
}

func TestFilterNotInt(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int: 2,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 4,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 5,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 7,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterNotInt("Int", 3, 4, 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
}

func TestFilterIntPtr(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(2),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(5),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(7),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterInt("IntPtr", 3, 4, 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
}

func TestFilterIntNull(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(2),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(5),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(7),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterIntNull("IntPtr")

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(4), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(4), results[0].ID)
}

func TestFilterNotIntNull(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(2),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(5),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(7),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterNotIntNull("IntPtr")

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(3), results[2].ID)
	assert.Equal(t, uint64(5), results[3].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(3), results[2].ID)
	assert.Equal(t, uint64(5), results[3].ID)
}

func TestFilterIntGreaterEqual(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(2),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(5),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(7),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterIntGreaterEqual("IntPtr", 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(5), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(5), results[1].ID)
}

func TestFilterIntGreater(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(2),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(5),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(7),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterIntGreater("IntPtr", 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(5), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(5), results[0].ID)
}

func TestFilterIntLessEqual(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(2),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(5),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(7),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterIntLessEqual("IntPtr", 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(3), results[2].ID)
	assert.Equal(t, uint64(4), results[3].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(3), results[2].ID)
	assert.Equal(t, uint64(4), results[3].ID)
}

func TestFilterIntLess(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(2),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(5),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(7),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterIntLess("IntPtr", 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(4), results[2].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(4), results[2].ID)
}

func TestFilterIntMinMax(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int: 2,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 4,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 5,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 7,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterIntMinMax("Int", 3, 6)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
}

func TestFilterIntMinMaxInclusive(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int: 2,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 4,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 5,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 7,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterIntMinMax("Int", 4, 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
}

func TestFilterFloatMinMax(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Float: 4.1,
	})
	engine.Flush(&entity.TestEntityOne{
		Float: 4.3,
	})
	engine.Flush(&entity.TestEntityOne{
		Float: 4.4,
	})
	engine.Flush(&entity.TestEntityOne{
		Float: 4.6,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterFloatMinMax("Float", 4.2, 4.5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
}

func TestFilterFloatMinMaxInclusive(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Float: 4.1,
	})
	engine.Flush(&entity.TestEntityOne{
		Float: 4.3,
	})
	engine.Flush(&entity.TestEntityOne{
		Float: 4.4,
	})
	engine.Flush(&entity.TestEntityOne{
		Float: 4.6,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterFloatMinMax("Float", 4.3, 4.4)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
}

func TestFilterFloat(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Float: 4.1,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterFloat("Float", 4.1)

	result := &entity.TestEntityOne{}
	assert.True(t, redisSearch.RedisSearchOne(result, q))

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	result = &entity.TestEntityOne{}
	assert.True(t, redisSearch.RedisSearchOne(result, q))
}

func TestFilterFloatPtr(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		FloatPtr: pointer.Float64(4.1),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterFloat("FloatPtr", 4.1)

	result := &entity.TestEntityOne{}
	assert.True(t, redisSearch.RedisSearchOne(result, q))

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	result = &entity.TestEntityOne{}
	assert.True(t, redisSearch.RedisSearchOne(result, q))
}

func TestFilterFloatGreaterEqual(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Float: 4.1,
	})
	engine.Flush(&entity.TestEntityOne{
		Float: 3.1,
	})
	engine.Flush(&entity.TestEntityOne{
		Float: 2.1,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterFloatGreaterEqual("Float", 3.1)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
}

func TestFilterFloatGreater(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Float: 4.1,
	})
	engine.Flush(&entity.TestEntityOne{
		Float: 3.1,
	})
	engine.Flush(&entity.TestEntityOne{
		Float: 2.1,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterFloatGreater("Float", 3.1)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
}

func TestFilterFloatLessEqual(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Float: 4.1,
	})
	engine.Flush(&entity.TestEntityOne{
		Float: 3.1,
	})
	engine.Flush(&entity.TestEntityOne{
		Float: 2.1,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterFloatLessEqual("Float", 3.1)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
}

func TestFilterFloatLess(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Float: 4.1,
	})
	engine.Flush(&entity.TestEntityOne{
		Float: 3.1,
	})
	engine.Flush(&entity.TestEntityOne{
		Float: 2.1,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterFloatLess("Float", 3.1)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
}

func TestFilterFloatNull(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		FloatPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		FloatPtr: pointer.Float64(1),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterFloatNull("FloatPtr")

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
}

func TestFilterDateMinMax(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 10, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 11, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 13, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 14, 0, 0, 0, 0, time.UTC),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterDateMinMax("Time",
		time.Date(2000, 5, 11, 0, 0, 0, 0, time.UTC),
		time.Date(2000, 5, 13, 0, 0, 0, 0, time.UTC),
	)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
	assert.Equal(t, uint64(4), results[2].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
	assert.Equal(t, uint64(4), results[2].ID)
}

func TestFilterDate(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 10, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 11, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 13, 0, 0, 0, 0, time.UTC),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterDate("Time", time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC))

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
}

func TestFilterDatePtr(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		TimePtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		TimePtr: pointer.Time(time.Date(2000, 5, 11, 0, 0, 0, 0, time.UTC)),
	})
	engine.Flush(&entity.TestEntityOne{
		TimePtr: pointer.Time(time.Date(2000, 5, 12, 0, 0, 0, 0, time.UTC)),
	})
	engine.Flush(&entity.TestEntityOne{
		TimePtr: pointer.Time(time.Date(2000, 5, 12, 0, 0, 0, 0, time.UTC)),
	})
	engine.Flush(&entity.TestEntityOne{
		TimePtr: pointer.Time(time.Date(2000, 5, 13, 0, 0, 0, 0, time.UTC)),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterDate("TimePtr", time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC))

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
}

func TestFilterNotDate(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 10, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 11, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 13, 0, 0, 0, 0, time.UTC),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterNotDate("Time", time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC))

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(5), results[2].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(5), results[2].ID)
}

func TestFilterDateNull(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		TimePtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		TimePtr: pointer.Time(time.Date(2000, 5, 11, 0, 0, 0, 0, time.UTC)),
	})
	engine.Flush(&entity.TestEntityOne{
		TimePtr: pointer.Time(time.Date(2000, 5, 12, 0, 0, 0, 0, time.UTC)),
	})
	engine.Flush(&entity.TestEntityOne{
		TimePtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		TimePtr: pointer.Time(time.Date(2000, 5, 13, 0, 0, 0, 0, time.UTC)),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterDateNull("TimePtr")

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
}

func TestFilterNotDateNull(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		TimePtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		TimePtr: pointer.Time(time.Date(2000, 5, 11, 0, 0, 0, 0, time.UTC)),
	})
	engine.Flush(&entity.TestEntityOne{
		TimePtr: pointer.Time(time.Date(2000, 5, 12, 0, 0, 0, 0, time.UTC)),
	})
	engine.Flush(&entity.TestEntityOne{
		TimePtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		TimePtr: pointer.Time(time.Date(2000, 5, 13, 0, 0, 0, 0, time.UTC)),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterNotDateNull("TimePtr")

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
	assert.Equal(t, uint64(5), results[2].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
	assert.Equal(t, uint64(5), results[2].ID)
}

func TestFilterDateGreaterEqual(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 10, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 11, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 13, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 14, 0, 0, 0, 0, time.UTC),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterDateGreaterEqual("Time", time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC))

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
	assert.Equal(t, uint64(5), results[2].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
	assert.Equal(t, uint64(5), results[2].ID)
}

func TestFilterDateGreater(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 10, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 11, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 13, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 14, 0, 0, 0, 0, time.UTC),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterDateGreater("Time", time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC))

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(4), results[0].ID)
	assert.Equal(t, uint64(5), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(4), results[0].ID)
	assert.Equal(t, uint64(5), results[1].ID)
}

func TestFilterDateLessEqual(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 10, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 11, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 13, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 14, 0, 0, 0, 0, time.UTC),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterDateLessEqual("Time", time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC))

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(3), results[2].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(3), results[2].ID)
}

func TestFilterDateLess(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 10, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 11, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 13, 0, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 14, 0, 0, 0, 0, time.UTC),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterDateLess("Time", time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC))

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
}

func TestFilterDateTimeMinMax(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 1, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 3, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 4, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 5, 0, 0, 0, time.UTC),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterDateTimeMinMax(
		"Time",
		time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC),
		time.Date(2000, 5, 12, 4, 0, 0, 0, time.UTC),
	)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
	assert.Equal(t, uint64(4), results[2].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
	assert.Equal(t, uint64(4), results[2].ID)
}

func TestFilterDateTime(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 1, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 3, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 4, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 5, 0, 0, 0, time.UTC),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterDateTime("Time", time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC))

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
}

func TestFilterDateTimeGreaterEqual(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 1, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 3, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 4, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 5, 0, 0, 0, time.UTC),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterDateTimeGreaterEqual("Time", time.Date(2000, 5, 12, 3, 0, 0, 0, time.UTC))

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
	assert.Equal(t, uint64(5), results[2].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
	assert.Equal(t, uint64(5), results[2].ID)
}

func TestFilterDateTimeGreater(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 1, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 3, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 4, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 5, 0, 0, 0, time.UTC),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterDateTimeGreater("Time", time.Date(2000, 5, 12, 3, 0, 0, 0, time.UTC))

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(4), results[0].ID)
	assert.Equal(t, uint64(5), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(4), results[0].ID)
	assert.Equal(t, uint64(5), results[1].ID)
}

func TestFilterDateTimeLessEqual(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 1, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 3, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 4, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 5, 0, 0, 0, time.UTC),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterDateTimeLessEqual("Time", time.Date(2000, 5, 12, 3, 0, 0, 0, time.UTC))

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(3), results[2].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(3), results[2].ID)
}

func TestFilterDateTimeLess(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 1, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 2, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 3, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 4, 0, 0, 0, time.UTC),
	})
	engine.Flush(&entity.TestEntityOne{
		Time: time.Date(2000, 5, 12, 5, 0, 0, 0, time.UTC),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterDateTimeLess("Time", time.Date(2000, 5, 12, 3, 0, 0, 0, time.UTC))

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
}

func TestFilterTag(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		StringEnum: entity.TestEntityEnumOne,
	})
	engine.Flush(&entity.TestEntityOne{
		StringEnum: entity.TestEntityEnumTwo,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterTag("StringEnum", entity.TestEntityEnumTwo)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
}

func TestFilterNotTag(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		StringEnum: entity.TestEntityEnumOne,
	})
	engine.Flush(&entity.TestEntityOne{
		StringEnum: entity.TestEntityEnumTwo,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterNotTag("StringEnum", entity.TestEntityEnumTwo)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
}

func TestFilterTagPtr(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		StringEnumPtr: pointer.String(entity.TestEntityEnumOne),
	})
	engine.Flush(&entity.TestEntityOne{
		StringEnumPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		StringEnumPtr: pointer.String(entity.TestEntityEnumTwo),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterTag("StringEnumPtr", entity.TestEntityEnumTwo)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
}

func TestFilterNotTagPtr(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		StringEnumPtr: pointer.String(entity.TestEntityEnumOne),
	})
	engine.Flush(&entity.TestEntityOne{
		StringEnumPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		StringEnumPtr: pointer.String(entity.TestEntityEnumTwo),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterNotTag("StringEnumPtr", entity.TestEntityEnumTwo, "NULL")

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
}

func TestFilterTagNull(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		StringEnumPtr: pointer.String(entity.TestEntityEnumOne),
	})
	engine.Flush(&entity.TestEntityOne{
		StringEnumPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		StringEnumPtr: pointer.String(entity.TestEntityEnumTwo),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterTag("StringEnumPtr", entity.TestEntityEnumTwo, "")

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
}

func TestFilterBool(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Bool: false,
	})
	engine.Flush(&entity.TestEntityOne{
		Bool: true,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterBool("Bool", true)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
}

func TestFilterNotBool(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Bool: false,
	})
	engine.Flush(&entity.TestEntityOne{
		Bool: true,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterBool("Bool", false)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
}

func TestFilterBoolPtr(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		BoolPtr: pointer.Bool(false),
	})
	engine.Flush(&entity.TestEntityOne{
		BoolPtr: pointer.Bool(true),
	})
	engine.Flush(&entity.TestEntityOne{
		BoolPtr: nil,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterBool("BoolPtr", true)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
}

func TestFilterNotBoolPtr(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		BoolPtr: pointer.Bool(false),
	})
	engine.Flush(&entity.TestEntityOne{
		BoolPtr: pointer.Bool(true),
	})
	engine.Flush(&entity.TestEntityOne{
		BoolPtr: nil,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterBool("BoolPtr", false)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
}

func TestSortIntAsc(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int: 3,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 1,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 4,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 2,
	})

	q := redisearch.NewRedisSearchQuery()
	q.Sort("Int", false)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
	assert.Equal(t, uint64(1), results[2].ID)
	assert.Equal(t, uint64(3), results[3].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
	assert.Equal(t, uint64(1), results[2].ID)
	assert.Equal(t, uint64(3), results[3].ID)
}

func TestSortIntDesc(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int: 3,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 1,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 4,
	})
	engine.Flush(&entity.TestEntityOne{
		Int: 2,
	})

	q := redisearch.NewRedisSearchQuery()
	q.Sort("Int", true)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(1), results[1].ID)
	assert.Equal(t, uint64(4), results[2].ID)
	assert.Equal(t, uint64(2), results[3].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(1), results[1].ID)
	assert.Equal(t, uint64(4), results[2].ID)
	assert.Equal(t, uint64(2), results[3].ID)
}

func TestSortIntAscPtr(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(3),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(1),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(2),
	})

	q := redisearch.NewRedisSearchQuery()
	q.Sort("IntPtr", false)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
	assert.Equal(t, uint64(1), results[2].ID)
	assert.Equal(t, uint64(3), results[3].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
	assert.Equal(t, uint64(1), results[2].ID)
	assert.Equal(t, uint64(3), results[3].ID)
}

func TestSortIntDescPtr(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(3),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(1),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		IntPtr: pointer.Int64(2),
	})

	q := redisearch.NewRedisSearchQuery()
	q.Sort("IntPtr", true)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(1), results[1].ID)
	assert.Equal(t, uint64(4), results[2].ID)
	assert.Equal(t, uint64(2), results[3].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(1), results[1].ID)
	assert.Equal(t, uint64(4), results[2].ID)
	assert.Equal(t, uint64(2), results[3].ID)
}

func TestFilterUint(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{})
	engine.Flush(&entity.TestEntityOne{})
	engine.Flush(&entity.TestEntityOne{})
	engine.Flush(&entity.TestEntityOne{})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("ID", 3, 4, 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
}

func TestFilterUintPtr(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(2),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(5),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(7),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("UintPtr", 3, 4, 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
}

func TestFilterUintNull(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(2),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(5),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(7),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUintNull("UintPtr")

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(4), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(4), results[0].ID)
}

func TestFilterUintGreaterEqual(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(2),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(5),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(7),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUintGreaterEqual("UintPtr", 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(5), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(5), results[1].ID)
}

func TestFilterUintGreater(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(2),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(5),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(7),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUintGreater("UintPtr", 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(5), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(5), results[0].ID)
}

func TestFilterUintLessEqual(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(2),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(5),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(7),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUintLessEqual("UintPtr", 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(3), results[2].ID)
	assert.Equal(t, uint64(4), results[3].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(3), results[2].ID)
	assert.Equal(t, uint64(4), results[3].ID)
}

func TestFilterUintLess(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(2),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(4),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(5),
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: nil,
	})
	engine.Flush(&entity.TestEntityOne{
		UintPtr: pointer.Uint64(7),
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUintLess("UintPtr", 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(4), results[2].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(1), results[0].ID)
	assert.Equal(t, uint64(2), results[1].ID)
	assert.Equal(t, uint64(4), results[2].ID)
}

func TestFilterUintMinMax(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{})
	engine.Flush(&entity.TestEntityOne{})
	engine.Flush(&entity.TestEntityOne{})
	engine.Flush(&entity.TestEntityOne{})
	engine.Flush(&entity.TestEntityOne{})
	engine.Flush(&entity.TestEntityOne{})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUintMinMax("ID", 2, 5)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
	assert.Equal(t, uint64(4), results[2].ID)
	assert.Equal(t, uint64(5), results[3].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(4), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(3), results[1].ID)
	assert.Equal(t, uint64(4), results[2].ID)
	assert.Equal(t, uint64(5), results[3].ID)
}

func TestFilterIntWithPrefixSearch(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{Int: 2, String: "anton"})
	engine.Flush(&entity.TestEntityOne{Int: 2, String: "iliyan"})
	engine.Flush(&entity.TestEntityOne{Int: 2, String: "krasi"})
	engine.Flush(&entity.TestEntityOne{Int: 2, String: "iliyan-1"})
	engine.Flush(&entity.TestEntityOne{Int: 3, String: "iliyan"})
	engine.Flush(&entity.TestEntityOne{Int: 3, String: "anton"})

	q := redisearch.NewRedisSearchQuery()
	q.FilterInt("Int", 2)
	q.AppendQueryRaw("@String: *iyan")

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
}

func TestFilterIntWithSuffixSearch(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{Int: 2, String: "anton"})
	engine.Flush(&entity.TestEntityOne{Int: 2, String: "iliyan"})
	engine.Flush(&entity.TestEntityOne{Int: 2, String: "krasi"})
	engine.Flush(&entity.TestEntityOne{Int: 2, String: "iliyan-1"})
	engine.Flush(&entity.TestEntityOne{Int: 3, String: "iliyan"})
	engine.Flush(&entity.TestEntityOne{Int: 3, String: "anton"})

	q := redisearch.NewRedisSearchQuery()
	q.FilterInt("Int", 2)
	q.AppendQueryRaw("@String: iliya*")
	q.Sort("Int", false)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
}

func TestFilterIntWithPrefixAndSuffixSearch(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{Int: 2, String: "anton"})
	engine.Flush(&entity.TestEntityOne{Int: 2, String: "iliyan"})
	engine.Flush(&entity.TestEntityOne{Int: 2, String: "krasi"})
	engine.Flush(&entity.TestEntityOne{Int: 2, String: "iliyan-1"})
	engine.Flush(&entity.TestEntityOne{Int: 3, String: "iliyan"})
	engine.Flush(&entity.TestEntityOne{Int: 3, String: "anton"})

	q := redisearch.NewRedisSearchQuery()
	q.FilterInt("Int", 2)
	q.AppendQueryRaw("@String: *liya*")
	q.Sort("Int", false)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
}

func TestPrefixAndSuffixSearch(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{Int: 2, String: "anton"})
	engine.Flush(&entity.TestEntityOne{Int: 2, String: "iliyan"})
	engine.Flush(&entity.TestEntityOne{Int: 2, String: "krasi"})
	engine.Flush(&entity.TestEntityOne{Int: 2, String: "iliyan-1"})
	engine.Flush(&entity.TestEntityOne{Int: 3, String: "iliyan"})
	engine.Flush(&entity.TestEntityOne{Int: 3, String: "anton"})

	q := redisearch.NewRedisSearchQuery()
	q.QueryRaw("@String: *liya*")
	q.Sort("Int", false)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
	assert.Equal(t, uint64(5), results[2].ID)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(3), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, uint64(2), results[0].ID)
	assert.Equal(t, uint64(4), results[1].ID)
	assert.Equal(t, uint64(5), results[2].ID)
}

func TestSearchForReference(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	entityRef1 := &entity.TestEntityTwo{Field: "test1"}
	entityRef2 := &entity.TestEntityTwo{Field: "test2"}

	engine.Flush(entityRef1)
	engine.Flush(entityRef2)

	engine.Flush(&entity.TestEntityOne{ForeignKey: entityRef1})
	engine.Flush(&entity.TestEntityOne{ForeignKey: entityRef2})
	engine.Flush(&entity.TestEntityOne{ForeignKey: entityRef1})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("ForeignKey", entityRef1.ID)
	q.Sort("ForeignKey", true)

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results, "ForeignKey"))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(1), results[1].ID)
	assert.Equal(t, "test1", results[0].ForeignKey.Field)
	assert.Equal(t, "test1", results[1].ForeignKey.Field)

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	results = make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(2), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results, "ForeignKey"))

	assert.Equal(t, uint64(3), results[0].ID)
	assert.Equal(t, uint64(1), results[1].ID)
	assert.Equal(t, "test1", results[0].ForeignKey.Field)
	assert.Equal(t, "test1", results[1].ForeignKey.Field)
}

func TestCustomIndex(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:        1,
		IntPtr:     pointer.Int64(2),
		Float:      3,
		String:     "test string 1",
		Bool:       true,
		StringEnum: entity.TestEntityEnumOne,
	})

	reindexCustomIndexEntityOne(engine)

	q := redisearch.NewRedisSearchQuery()
	q.FilterString("String", "test string 1")

	ids, total := redisearch.GetEntityIDs(redisSearch, customindex.EntityOneCustomIndex, q, beeorm.NewPager(1, 1000))
	assert.Equal(t, uint64(1), total)

	result := &entity.TestEntityOne{}
	assert.True(t, engine.LoadByID(ids[0], result))
}

func TestCustomIndexWithGeoField(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:        1,
		IntPtr:     pointer.Int64(2),
		Float:      3,
		String:     "test string 1",
		Bool:       true,
		StringEnum: entity.TestEntityEnumOne,
	})

	reindexCustomIndexEntityOne(engine)

	q := redisearch.NewRedisSearchQuery()
	q.FilterGeo("Geo", 1.2, 1.5, 100, "km")

	ids, total := redisearch.GetEntityIDs(redisSearch, customindex.EntityOneCustomIndex, q, beeorm.NewPager(1, 1000))
	assert.Equal(t, uint64(1), total)

	result := &entity.TestEntityOne{}
	assert.True(t, engine.LoadByID(ids[0], result))
}

func TestReindexCustomIndex(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:        1,
		IntPtr:     pointer.Int64(2),
		Float:      3,
		String:     "test string 1",
		Bool:       true,
		StringEnum: entity.TestEntityEnumOne,
	})

	reindexCustomIndexEntityOne(engine)

	redisSearch.HandleRedisIndexerEvent(customindex.EntityOneCustomIndex)

	q := redisearch.NewRedisSearchQuery()
	q.FilterString("String", "test string 1")

	ids, total := redisearch.GetEntityIDs(redisSearch, customindex.EntityOneCustomIndex, q, beeorm.NewPager(1, 1000))
	assert.Equal(t, uint64(1), total)

	result := &entity.TestEntityOne{}
	assert.True(t, engine.LoadByID(ids[0], result))
}

func TestReindexCustomIndexWithFakeDeleted(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:        1,
		IntPtr:     pointer.Int64(2),
		Float:      3,
		String:     "test string 1",
		Bool:       true,
		StringEnum: entity.TestEntityEnumOne,
	})

	redisSearch.HandleRedisIndexerEvent(customindex.EntityOneCustomIndex)

	q := redisearch.NewRedisSearchQuery()
	q.FilterString("String", "test string 1")

	ids, total := redisearch.GetEntityIDs(redisSearch, customindex.EntityOneCustomIndex, q, beeorm.NewPager(1, 1000))
	assert.Equal(t, uint64(1), total)

	result := &entity.TestEntityOne{}
	assert.True(t, engine.LoadByID(ids[0], result))

	engine.Delete(result)

	redisSearch.HandleRedisIndexerEvent(customindex.EntityOneCustomIndex)

	ids, total = redisearch.GetEntityIDs(redisSearch, customindex.EntityOneCustomIndex, q, beeorm.NewPager(1, 1000))
	assert.Equal(t, uint64(0), total)
	assert.Equal(t, 0, len(ids))
}

func TestReindexNormalIndex(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	testEntityFK := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	engine.Flush(testEntityFK)

	engine.Flush(&entity.TestEntityOne{
		UintPtr:       pointer.Uint64(1),
		Int:           2,
		IntPtr:        pointer.Int64(3),
		Float:         4.1,
		FloatPtr:      pointer.Float64(5.1),
		String:        "test string 1",
		StringPtr:     pointer.String("test string 2"),
		StringEnum:    entity.TestEntityEnumOne,
		StringEnumPtr: pointer.String(entity.TestEntityEnumTwo),
		StringSlice:   []string{"item 1", "item 2"},
		Bool:          true,
		BoolPtr:       pointer.Bool(true),
		Time:          time.Unix(100, 0).UTC(),
		TimePtr:       pointer.Time(time.Unix(100, 0).UTC()),
		ForeignKey:    testEntityFK,
		Many:          []*entity.TestEntityTwo{testEntityFK},
	})

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	q := redisearch.NewRedisSearchQuery()
	q.FilterString("String", "test string 1")

	result := &entity.TestEntityOne{}
	assert.True(t, redisSearch.RedisSearchOne(result, q, "ForeignKey"))
}

func TestFilterManyReferenceIn(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	testEntityFK1 := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	testEntityFK2 := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	engine.Flush(testEntityFK1, testEntityFK2)

	engine.Flush(&entity.TestEntityOne{
		Many: []*entity.TestEntityTwo{testEntityFK1, testEntityFK2},
	})

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	q := redisearch.NewRedisSearchQuery()
	q.FilterManyReferenceIn("Many", uint64(1))

	result := &entity.TestEntityOne{}
	assert.True(t, redisSearch.RedisSearchOne(result, q))
}

func TestFilterManyReferenceNotIn(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	testEntityFK1 := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	testEntityFK2 := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	engine.Flush(testEntityFK1, testEntityFK2)

	engine.Flush(&entity.TestEntityOne{
		String: "s1",
		Many:   []*entity.TestEntityTwo{testEntityFK1},
	})

	engine.Flush(&entity.TestEntityOne{
		String: "s2",
		Many:   []*entity.TestEntityTwo{testEntityFK2},
	})

	redisSearch.HandleRedisIndexerEvent("entity.TestEntityOne")

	q := redisearch.NewRedisSearchQuery()
	q.FilterManyReferenceNotIn("Many", uint64(1))

	results := make([]*entity.TestEntityOne, 0)
	assert.Equal(t, uint64(1), redisSearch.RedisSearchMany(q, beeorm.NewPager(1, 100), &results))

	assert.Equal(t, "s2", results[0].String)
}

func TestGetRedisSearchStatistics(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	testEntityFK1 := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	testEntityFK2 := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	engine.Flush(testEntityFK1, testEntityFK2)

	engine.Flush(&entity.TestEntityOne{
		Many: []*entity.TestEntityTwo{testEntityFK1, testEntityFK2},
	})

	stats := redisSearch.GetRedisSearchStatistics()
	assert.Equal(t, 3, len(stats))
}

func TestGroupByFieldAggregateReduceSum(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 10,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Int", 1, 2)

	a := q.Aggregate()
	a.GroupByField("@Int", redisearch.NewAggregateReduceSum("@Float", "total_amount"))
	a.Sort(redisearch.RedisSearchAggregationSort{
		Field: "@total_amount",
		Desc:  true,
	})

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

	gotResellerBalanceMap := map[uint64]float64{}

	for _, row := range result {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row["Int"], 10, 64)
		if err != nil {
			panic(err)
		}

		amount, err := strconv.ParseFloat(row["total_amount"], 64)
		if err != nil {
			panic(err)
		}

		gotResellerBalanceMap[id] = amount
	}

	wantResellerBalanceMap := map[uint64]float64{
		1: 40,
		2: 20,
	}

	assert.Equal(t, wantResellerBalanceMap, gotResellerBalanceMap)
}

func TestGroupByFieldAggregateReduceSumWithLoad(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Int", 1, 2)

	a := q.Aggregate()

	load := &redisearch.LoadFields{}
	load.AddField("@Int")
	load.AddFieldWithAlias("@Float", "total_amount")

	a.Load(load)

	a.GroupByField("@Int", redisearch.NewAggregateReduceSum("@Float", "total_amount"))
	a.Sort(redisearch.RedisSearchAggregationSort{
		Field: "@total_amount",
		Desc:  true,
	})

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

	gotResellerBalanceMap := map[uint64]float64{}

	for _, row := range result {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row["Int"], 10, 64)
		if err != nil {
			panic(err)
		}

		amount, err := strconv.ParseFloat(row["total_amount"], 64)
		if err != nil {
			panic(err)
		}

		gotResellerBalanceMap[id] = amount
	}

	wantResellerBalanceMap := map[uint64]float64{
		1: 10,
	}

	assert.Equal(t, wantResellerBalanceMap, gotResellerBalanceMap)
}

func TestGroupByFieldAggregateReduceSumWithLoadAll(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Int", 1, 2)

	a := q.Aggregate()

	a.LoadAll()

	a.GroupByField("@Int", redisearch.NewAggregateReduceSum("@Float", "total_amount"))
	a.Sort(redisearch.RedisSearchAggregationSort{
		Field: "@total_amount",
		Desc:  false,
	})

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

	gotResellerBalanceMap := map[uint64]float64{}

	for _, row := range result {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row["Int"], 10, 64)
		if err != nil {
			panic(err)
		}

		amount, err := strconv.ParseFloat(row["total_amount"], 64)
		if err != nil {
			panic(err)
		}

		gotResellerBalanceMap[id] = amount
	}

	wantResellerBalanceMap := map[uint64]float64{
		1: 10,
	}

	assert.Equal(t, wantResellerBalanceMap, gotResellerBalanceMap)
}

func TestGroupByFieldAggregateReduceSumWithFilter(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 10,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Int", 1, 2)

	a := q.Aggregate()
	a.GroupByField("@Int", redisearch.NewAggregateReduceSum("@Float", "total_amount"))
	a.Filter("@Int < 2")

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

	gotResellerBalanceMap := map[uint64]float64{}

	for _, row := range result {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row["Int"], 10, 64)
		if err != nil {
			panic(err)
		}

		amount, err := strconv.ParseFloat(row["total_amount"], 64)
		if err != nil {
			panic(err)
		}

		gotResellerBalanceMap[id] = amount
	}

	wantResellerBalanceMap := map[uint64]float64{
		1: 40,
	}

	assert.Equal(t, wantResellerBalanceMap, gotResellerBalanceMap)
}

func TestAggregateApply(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Int", 1, 2)

	a := q.Aggregate()

	a.Apply("exists(@Int)", "exists")

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

	gotResellerBalanceMap := map[uint64]float64{}

	for _, row := range result {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row["Int"], 10, 64)
		if err != nil {
			panic(err)
		}

		amount, err := strconv.ParseFloat(row["exists"], 64)
		if err != nil {
			panic(err)
		}

		gotResellerBalanceMap[id] = amount
	}

	wantResellerBalanceMap := map[uint64]float64{
		1: 1,
	}

	assert.Equal(t, wantResellerBalanceMap, gotResellerBalanceMap)
}

func TestGroupByFieldAggregateReduceCount(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 10,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Int", 1, 2)

	a := q.Aggregate()
	a.GroupByField("@Int", redisearch.NewAggregateReduceCount("count"))

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

	gotResellerBalanceMap := map[uint64]float64{}

	for _, row := range result {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row["Int"], 10, 64)
		if err != nil {
			panic(err)
		}

		amount, err := strconv.ParseFloat(row["count"], 64)
		if err != nil {
			panic(err)
		}

		gotResellerBalanceMap[id] = amount
	}

	wantResellerBalanceMap := map[uint64]float64{
		1: 4,
		2: 2,
	}

	assert.Equal(t, wantResellerBalanceMap, gotResellerBalanceMap)
}

func TestGroupByFieldAggregateReduceCountDistinct(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 10,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Int", 1, 2)

	a := q.Aggregate()
	a.GroupByField("@Int", redisearch.NewAggregateReduceCountDistinct("@ID", "count", true))

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

	gotResellerBalanceMap := map[uint64]float64{}

	for _, row := range result {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row["Int"], 10, 64)
		if err != nil {
			panic(err)
		}

		amount, err := strconv.ParseFloat(row["count"], 64)
		if err != nil {
			panic(err)
		}

		gotResellerBalanceMap[id] = amount
	}

	wantResellerBalanceMap := map[uint64]float64{
		1: 2,
		2: 2,
	}

	assert.Equal(t, wantResellerBalanceMap, gotResellerBalanceMap)
}

func TestGroupByFieldAggregateReduceMin(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 20,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 30,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 40,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 20,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 30,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Int", 1, 2)

	a := q.Aggregate()
	a.GroupByField("@Int", redisearch.NewAggregateReduceMin("@Float", "min"))

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

	gotResellerBalanceMap := map[uint64]float64{}

	for _, row := range result {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row["Int"], 10, 64)
		if err != nil {
			panic(err)
		}

		amount, err := strconv.ParseFloat(row["min"], 64)
		if err != nil {
			panic(err)
		}

		gotResellerBalanceMap[id] = amount
	}

	wantResellerBalanceMap := map[uint64]float64{
		1: 10,
		2: 20,
	}

	assert.Equal(t, wantResellerBalanceMap, gotResellerBalanceMap)
}

func TestGroupByFieldAggregateReduceMax(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 20,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 30,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 40,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 20,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 30,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Int", 1, 2)

	a := q.Aggregate()
	a.GroupByField("@Int", redisearch.NewAggregateReduceMax("@Float", "max"))

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

	gotResellerBalanceMap := map[uint64]float64{}

	for _, row := range result {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row["Int"], 10, 64)
		if err != nil {
			panic(err)
		}

		amount, err := strconv.ParseFloat(row["max"], 64)
		if err != nil {
			panic(err)
		}

		gotResellerBalanceMap[id] = amount
	}

	wantResellerBalanceMap := map[uint64]float64{
		1: 40,
		2: 30,
	}

	assert.Equal(t, wantResellerBalanceMap, gotResellerBalanceMap)
}

func TestGroupByFieldAggregateReduceAvg(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 20,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 30,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 40,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 20,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 30,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Int", 1, 2)

	a := q.Aggregate()
	a.GroupByField("@Int", redisearch.NewAggregateReduceAvg("@Float", "avg"))

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

	gotResellerBalanceMap := map[uint64]float64{}

	for _, row := range result {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row["Int"], 10, 64)
		if err != nil {
			panic(err)
		}

		amount, err := strconv.ParseFloat(row["avg"], 64)
		if err != nil {
			panic(err)
		}

		gotResellerBalanceMap[id] = amount
	}

	wantResellerBalanceMap := map[uint64]float64{
		1: 25,
		2: 25,
	}

	assert.Equal(t, wantResellerBalanceMap, gotResellerBalanceMap)
}

func TestGroupByFieldAggregateReduceStdDev(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 20,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 30,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 40,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 20,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 30,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Int", 1, 2)

	a := q.Aggregate()
	a.GroupByField("@Int", redisearch.NewAggregateReduceStdDev("@Float", "std"))

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

	gotResellerBalanceMap := map[uint64]float64{}

	for _, row := range result {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row["Int"], 10, 64)
		if err != nil {
			panic(err)
		}

		amount, err := strconv.ParseFloat(row["std"], 64)
		if err != nil {
			panic(err)
		}

		gotResellerBalanceMap[id] = amount
	}

	wantResellerBalanceMap := map[uint64]float64{
		1: 12.9099444874,
		2: 7.07106781187,
	}

	assert.Equal(t, wantResellerBalanceMap, gotResellerBalanceMap)
}

func TestGroupByFieldAggregateReduceQuantile(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 20,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 30,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 40,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 20,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 30,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Int", 1, 2)

	a := q.Aggregate()
	a.GroupByField("@Int", redisearch.NewAggregateReduceQuantile("@Float", "0.5", "qq"))

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

	gotResellerBalanceMap := map[uint64]float64{}

	for _, row := range result {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row["Int"], 10, 64)
		if err != nil {
			panic(err)
		}

		amount, err := strconv.ParseFloat(row["qq"], 64)
		if err != nil {
			panic(err)
		}

		gotResellerBalanceMap[id] = amount
	}

	wantResellerBalanceMap := map[uint64]float64{
		1: 20,
		2: 20,
	}

	assert.Equal(t, wantResellerBalanceMap, gotResellerBalanceMap)
}

func TestGroupByFieldAggregateReduceToList(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 20,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 30,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 40,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 20,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 30,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Int", 1, 2)

	a := q.Aggregate()
	a.GroupByField("@Int", redisearch.NewAggregateReduceToList("@Float", "ll"))

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

	gotResellerBalanceMap := map[uint64]string{}

	for _, row := range result {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row["Int"], 10, 64)
		if err != nil {
			panic(err)
		}

		gotResellerBalanceMap[id] = row["ll"]
	}

	wantResellerBalanceMap := map[uint64]string{
		1: "10,20,40,30",
		2: "20,30",
	}

	assert.Equal(t, wantResellerBalanceMap, gotResellerBalanceMap)
}

func TestGroupByFiNewAggregateReduceFirstValue(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 10,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 20,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 30,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   1,
		Float: 40,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 20,
	})
	engine.Flush(&entity.TestEntityOne{
		Int:   2,
		Float: 30,
	})

	q := redisearch.NewRedisSearchQuery()
	q.FilterUint("Int", 1, 2)

	a := q.Aggregate()
	a.GroupByField("@Int", redisearch.NewAggregateReduceFirstValue("@Float", "fv"))

	result, _ := redisSearch.RedisSearchAggregate(&entity.TestEntityOne{}, a, beeorm.NewPager(1, 1000))

	gotResellerBalanceMap := map[uint64]float64{}

	for _, row := range result {
		if len(row) < 2 {
			continue
		}

		id, err := strconv.ParseUint(row["Int"], 10, 64)
		if err != nil {
			panic(err)
		}

		amount, err := strconv.ParseFloat(row["fv"], 64)
		if err != nil {
			panic(err)
		}

		gotResellerBalanceMap[id] = amount
	}

	wantResellerBalanceMap := map[uint64]float64{
		1: 10,
		2: 20,
	}

	assert.Equal(t, wantResellerBalanceMap, gotResellerBalanceMap)
}

func TestSearchRaw(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	testEntityFK := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	engine.Flush(testEntityFK)

	engine.Flush(&entity.TestEntityOne{
		String:     "test string 1",
		ForeignKey: testEntityFK,
	})

	q := redisearch.NewRedisSearchQuery()
	q.QueryField("String", "test string 1")

	total, _ := redisSearch.SearchRaw("entity.TestEntityOne", q, beeorm.NewPager(1, 100))

	assert.Equal(t, uint64(1), total)
}

func TestSearchCount(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	testEntityFK := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	engine.Flush(testEntityFK)

	engine.Flush(&entity.TestEntityOne{
		String:     "test string 1",
		ForeignKey: testEntityFK,
	})

	q := redisearch.NewRedisSearchQuery()
	q.QueryField("String", "test string 1")

	count := redisSearch.SearchCount("entity.TestEntityOne", q)

	assert.Equal(t, uint64(1), count)
}

func TestSearchResult(t *testing.T) {
	engine, redisSearch := createTestEngine(context.Background())

	testEntityFK := &entity.TestEntityTwo{
		Field: "test string 1",
	}

	engine.Flush(testEntityFK)

	engine.Flush(&entity.TestEntityOne{
		String:     "test string 1",
		ForeignKey: testEntityFK,
	})

	q := redisearch.NewRedisSearchQuery()
	q.QueryField("String", "test string 1")

	count, rows := redisSearch.SearchResult("entity.TestEntityOne", q, beeorm.NewPager(1, 100))

	assert.Equal(t, uint64(1), count)
	assert.Equal(t, 1, len(rows))
}

func TestGetRedisSearchAlters(t *testing.T) {
	_, redisSearch := createTestEngine(context.Background())
	assert.Equal(t, 0, len(redisSearch.GetRedisSearchAlters()))
}
