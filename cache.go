package cache

import (
	"errors"
	"time"

	"github.com/go-mego/mego"
)

var (
	ErrNotFound       = errors.New("cache: the key is not found")
	ErrDatabaseClosed = errors.New("cache: the database connection is closed")
)

func New(options ...*Options) mego.HandlerFunc {
	var o *Options
	o = &Options{
		Source: SourceTypeMemory,
	}
	if len(options) != 0 {
		o = options[0]
	}
	var s Store
	switch o.Source {
	case SourceTypeMemory:
		s = newMemory(o)
	case SourceTypeMySQL:
		s = newMySQL(o)
	case SourceTypeSQLite:
		s = newSQLite(o)
	case SourceTypeMemcache:
		s = newMemcache(o)
	}
	return func(c *mego.Context) {
		c.Map(s)
		c.Next()
	}
}

type item struct {
	value     string
	createdAt time.Time
	ttl       time.Duration
}

type SourceType int

const (
	SourceTypeMemory SourceType = iota
	SourceTypeMySQL
	SourceTypeSQLite
	SourceTypeMemcache
)

// Options 是快取的設置選項。
type Options struct {
	// Source 是快取存放來源。
	Source SourceType
	// SourceConfig 是來源設置，不同快取存放來源有不同的設置方法。
	SourceConfig string
}

// SetOptions 是設置快取值時所可傳入的選項。
type SetOptions struct {
	// TTL 是此值的存活時間，超過此時間此值則會過期無法取得（除非有設置忽略過期選項）。
	// 留白則為永久直到快取被清空。
	TTL time.Duration
}

// Store 是個通用的快取存取介面。
type Store interface {
	Set(key string, value string, options ...*SetOptions) error
	Get(key string) (string, error)
	Has(key string) (bool, error)
	Delete(key string) error
	DeleteAll() error
	Decrease(key string, count int) error
	Increase(key string, count int) error
	TTL(key string) (time.Duration, error)
}
