package redisearch

import "github.com/latolukasz/beeorm/v2"

type RedisSearchIndex struct {
	Name            string
	RedisPool       string
	Prefixes        []string
	DefaultLanguage string
	LanguageField   string
	DefaultScore    float64
	ScoreField      string
	MaxTextFields   bool
	NoOffsets       bool
	NoNHL           bool
	NoFields        bool
	NoFreqs         bool
	SkipInitialScan bool
	StopWords       []string
	Fields          []RedisSearchIndexField
	Indexer         RedisSearchIndexerFunc `json:"-"`
}

type RedisSearchIndexField struct {
	Type         string
	Name         string
	Sortable     bool
	NoIndex      bool
	NoStem       bool
	Weight       float64
	TagSeparator string
}

type RedisSearchIndexerFunc func(engine beeorm.Engine, lastID uint64, pusher RedisSearchIndexPusher) (newID uint64, hasMore bool)

func (rs *RedisSearchIndex) AddTextField(name string, weight float64, sortable, noindex, nostem bool) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:     redisSearchIndexFieldText,
		Name:     name,
		Sortable: sortable,
		NoIndex:  noindex,
		NoStem:   nostem,
		Weight:   weight,
	})
}

func (rs *RedisSearchIndex) AddNumericField(name string, sortable, noindex bool) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:     redisSearchIndexFieldNumeric,
		Name:     name,
		Sortable: sortable,
		NoIndex:  noindex,
	})
}

func (rs *RedisSearchIndex) AddGeoField(name string, sortable, noindex bool) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:     redisSearchIndexFieldGeo,
		Name:     name,
		Sortable: sortable,
		NoIndex:  noindex,
	})
}

func (rs *RedisSearchIndex) AddTagField(name string, sortable, noindex bool, separator string) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:         redisSearchIndexFieldTAG,
		Name:         name,
		Sortable:     sortable,
		NoIndex:      noindex,
		TagSeparator: separator,
	})
}
