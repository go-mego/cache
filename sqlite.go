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

func (s *sqlite) Set(key string, value string, options ...*SetOptions) error {
	ttl := time.Duration(-1)
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
	rows, err := s.db.Query("SELECT value FROM cache WHERE key = ? AND (createdAt + ttl > ? OR ttl = -1)", key, time.Now().Unix())
	if err != nil {
		return "", err
	}
	var value string
	var count int
	for rows.Next() {
		count++
		err = rows.Scan(&value)
		if err != nil {
			return "", err
		}
	}
	if count <= 0 {
		return "", ErrNotFound
	}
	return value, nil
}

func (s *sqlite) Has(key string) (bool, error) {
	rows, err := s.db.Query("SELECT COUNT(*) FROM cache WHERE key = ? AND (createdAt + ttl > ? OR ttl = -1) GROUP BY key", key, time.Now().Unix())
	if err != nil {
		return false, err
	}
	var count int
	for rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			return false, err
		}
	}
	if count <= 0 {
		return false, nil
	}
	return true, nil
}

func (s *sqlite) Delete(key string) error {
	res, err := s.db.Exec("DELETE FROM cache WHERE key = ? AND (createdAt + ttl > ? OR ttl = -1)", key, time.Now().Unix())
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
	res, err := s.db.Exec("UPDATE cache SET value = value - ? WHERE key = ? AND (createdAt + ttl > ? OR ttl = -1)", count, key, time.Now().Unix())
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

func (s *sqlite) Increase(key string, count int) error {
	res, err := s.db.Exec("UPDATE cache SET value = value + ? WHERE key = ? AND (createdAt + ttl > ? OR ttl = -1)", count, key, time.Now().Unix())
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

func (s *sqlite) TTL(key string) (time.Duration, error) {
	rows, err := s.db.Query("SELECT createdAt, ttl FROM cache WHERE key = ? AND (createdAt + ttl > ? OR ttl = -1)", key, time.Now().Unix())
	if err != nil {
		return -1, err
	}
	var createdAt, ttl int
	var count int
	for rows.Next() {
		count++
		err = rows.Scan(&createdAt, &ttl)
		if err != nil {
			return -1, err
		}
	}
	if count <= 0 {
		return -1, ErrNotFound
	}
	return time.Duration(int64(ttl)) - time.Since(time.Unix(int64(createdAt), 0)), nil
}
