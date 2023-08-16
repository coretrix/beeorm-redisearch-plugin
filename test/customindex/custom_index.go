package customindex

import (
	"strconv"

	"github.com/latolukasz/beeorm/v2"

	redisearch "github.com/coretrix/beeorm-redisearch-plugin"
	"github.com/coretrix/beeorm-redisearch-plugin/test/entity"
)

const EntityOneCustomIndex = "custom_index_entity_one"

func GetEntityOneIndex(redisSearchPool string) *redisearch.RedisSearchIndex {
	index := &redisearch.RedisSearchIndex{}
	index.Name = EntityOneCustomIndex
	index.RedisPool = redisSearchPool
	index.Prefixes = []string{EntityOneCustomIndex + ":"}

	// document fields
	index.AddNumericField("ID", true, false)
	index.AddNumericField("Int", true, false)
	index.AddNumericField("IntPtr", true, false)
	index.AddNumericField("Float", true, false)
	index.AddTextField("String", 1, true, false, false)
	index.AddTagField("Bool", true, false, ",")
	index.AddTagField("StringEnum", true, false, ",")
	index.AddGeoField("Geo", true, false)

	// force reindex func
	index.Indexer = entityOneIndexer

	return index
}

func SetEntityOneIndexFields(pusher redisearch.RedisSearchIndexPusher, entities []*entity.TestEntityOne) {
	deletedIDs := make([]string, 0)

	for _, entityIter := range entities {
		id := EntityOneCustomIndex + ":" + strconv.FormatUint(entityIter.ID, 10)

		if entityIter.FakeDelete {
			deletedIDs = append(deletedIDs, id)

			continue
		}

		pusher.NewDocument(id)
		pusher.SetUint("ID", entityIter.ID)
		pusher.SetInt("Int", entityIter.Int)
		pusher.SetIntNil("IntPtr")
		pusher.SetFloat("Float", entityIter.Float)
		pusher.SetString("String", entityIter.String)
		pusher.SetBool("Bool", entityIter.Bool)
		pusher.SetTag("StringEnum", entityIter.StringEnum)
		pusher.SetGeo("Geo", 1.2, 1.5)
		pusher.PushDocument()
	}

	if len(deletedIDs) != 0 {
		pusher.DeleteDocuments(deletedIDs...)
	}
}

func entityOneIndexer(engine beeorm.Engine, lastID uint64, pusher redisearch.RedisSearchIndexPusher) (newID uint64, hasMore bool) {
	where := beeorm.NewWhere("`ID` > ? AND `FakeDelete` >= 0 ORDER BY `ID` ASC", lastID)

	entities := make([]*entity.TestEntityOne, 0)
	engine.Search(where, beeorm.NewPager(1, 1000), &entities)

	if len(entities) == 0 {
		return lastID, false
	}

	SetEntityOneIndexFields(pusher, entities)
	pusher.Flush()

	lastID = entities[len(entities)-1].ID

	return lastID, !(len(entities) < 1000)
}
