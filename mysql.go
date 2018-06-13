package cache

import (
	"database/sql"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func newMySQL(options *Options) Store {
	db, err := sql.Open("mysql", options.SourceConfig)
	if err != nil {
		panic(err)
	}
	_, err = db.Exec(`CREATE TABLE cache IF NOT EXISTS (
		key varchar(128),
		value text,
		ttl int(10),
		createdAt int(10),
		PRIMARY KEY (Key)
	) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;`)
	if err != nil {
		panic(err)
	}
	return &mysql{
		db: db,
	}
}

type mysql struct {
	db *sql.DB
}

func (m *mysql) Set(key string, value string, options ...*SetOptions) error {
	ttl := time.Duration(-1)
	if len(options) > 0 {
		ttl = options[0].TTL
	}
	_, err := m.db.Exec("INSERT INTO cache (key, value, createdAt, ttl) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE value = ?, createdAt = ?, ttl = ?", key, value, time.Now().Unix(), ttl.Seconds(), value, time.Now().Unix(), ttl.Seconds())
	if err != nil {
		return err
	}
	return nil
}

func (m *mysql) Get(key string) (string, error) {
	rows, err := m.db.Query("SELECT value FROM cache WHERE key = ? AND (createdAt + ttl > ? OR ttl = -1)", key, time.Now().Unix())
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

func (m *mysql) Has(key string) (bool, error) {
	rows, err := m.db.Query("SELECT COUNT(*) as count FROM cache WHERE key = ? AND (createdAt + ttl > ? OR ttl = -1)", key, time.Now().Unix())
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

func (m *mysql) Delete(key string) error {
	res, err := m.db.Exec("DELETE FROM cache WHERE key = ? AND (createdAt + ttl > ? OR ttl = -1)", key, time.Now().Unix())
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

func (m *mysql) DeleteAll() error {
	res, err := m.db.Exec("DELETE FROM cache")
	if err != nil {
		return err
	}
	_, err = res.RowsAffected()
	if err != nil {
		return err
	}
	return nil
}

func (m *mysql) Decrease(key string, count int) error {
	res, err := m.db.Exec("UPDATE cache SET value = value - ? WHERE key = ? AND (createdAt + ttl > ? OR ttl = -1)", count, key, time.Now().Unix())
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

func (m *mysql) Increase(key string, count int) error {
	res, err := m.db.Exec("UPDATE cache SET value = value + ? WHERE key = ? AND (createdAt + ttl > ? OR ttl = -1)", count, key, time.Now().Unix())
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

func (m *mysql) TTL(key string) (time.Duration, error) {
	rows, err := m.db.Query("SELECT createdAt, ttl FROM cache WHERE key = ? AND (createdAt + ttl > ? OR ttl = -1)", key, time.Now().Unix())
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
