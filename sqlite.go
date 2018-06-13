package cache

import (
	"database/sql"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func newSQLite(options *Options) Store {
	db, err := sql.Open("sqlite3", options.SourceConfig)
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS cache (
		key INTEGER PRIMARY KEY,
		value TEXT,
		ttl INTEGER,
		createdAt INTEGER
	)`)
	if err != nil {
		panic(err)
	}
	return &sqlite{
		db: db,
	}
}

type sqlite struct {
	db *sql.DB
}

func (s *sqlite) get(key string) (item, error) {
	rows, err := s.db.Query("SELECT value, createdAt, ttl FROM cache WHERE key = ?", key)
	if err != nil {
		return item{}, err
	}
	var i item
	var createdAt, ttl int64
	var count int
	for rows.Next() {
		count++
		err = rows.Scan(&i.value, &createdAt, &ttl)
		if err != nil {
			return item{}, err
		}
	}
	if count <= 0 {
		return item{}, ErrNotFound
	}
	i = item{
		value:     i.value,
		createdAt: time.Unix(createdAt, 0),
		ttl:       time.Duration(ttl),
	}
	if i.ttl > 0 && time.Since(i.createdAt) > i.ttl {
		s.Delete(key)
		return item{}, ErrNotFound
	}
	return i, nil
}

func (s *sqlite) Set(key string, value string, options ...*SetOptions) error {
	ttl := time.Duration(0)
	if len(options) > 0 {
		ttl = options[0].TTL
	}
	_, err := s.db.Exec("INSERT OR REPLACE INTO cache(key, value, createdAt, ttl) VALUES(?, ?, ?, ?)", key, value, time.Now().Unix(), ttl.Seconds())
	if err != nil {
		return err
	}
	return nil
}

func (s *sqlite) Get(key string) (string, error) {
	i, err := s.get(key)
	if err != nil {
		return "", err
	}
	return i.value, nil
}

func (s *sqlite) Has(key string) (bool, error) {
	_, err := s.get(key)
	if err != nil {
		if err == ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (s *sqlite) Delete(key string) error {
	res, err := s.db.Exec("DELETE FROM cache WHERE key = ?", key)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected <= 0 {
		return ErrNotFound
	}
	return nil
}

func (s *sqlite) DeleteAll() error {
	res, err := s.db.Exec("DELETE FROM cache")
	if err != nil {
		return err
	}
	_, err = res.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}

func (s *sqlite) Decrease(key string, count int) error {
	has, err := s.Has(key)
	if err != nil {
		return err
	}
	if !has {
		return ErrNotFound
	}
	_, err = s.db.Exec("UPDATE cache SET value = value - ? WHERE key = ?", count, key)
	if err != nil {
		return err
	}
	return nil
}

func (s *sqlite) Increase(key string, count int) error {
	has, err := s.Has(key)
	if err != nil {
		return err
	}
	if !has {
		return ErrNotFound
	}
	_, err = s.db.Exec("UPDATE cache SET value = value + ? WHERE key = ?", count, key)
	if err != nil {
		return err
	}
	return nil
}

func (s *sqlite) TTL(key string) (time.Duration, error) {
	i, err := s.get(key)
	if err != nil {
		return 0, err
	}
	return i.ttl - time.Since(i.createdAt), nil
}
