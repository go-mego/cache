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
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS cache (
		.key varchar(128) PRIMARY KEY,
		value text,
		ttl int(10),
		createdAt int(10)
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
	ttl := time.Duration(0)
	if len(options) > 0 {
		ttl = options[0].TTL
	}
	_, err := m.db.Exec("INSERT INTO cache (`key`, value, createdAt, ttl) VALUES (?, ?, ?, ?) ON DUPLICATE KEY UPDATE value = ?, createdAt = ?, ttl = ?", key, value, time.Now().Unix(), ttl.Seconds(), value, time.Now().Unix(), ttl.Seconds())
	if err != nil {
		return err
	}
	return nil
}

func (m *mysql) Get(key string) (string, error) {
	i, err := m.get(key)
	if err != nil {
		return "", err
	}
	return i.value, nil
}

func (m *mysql) Has(key string) (bool, error) {
	_, err := m.get(key)
	if err != nil {
		if err == ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (m *mysql) Delete(key string) error {
	res, err := m.db.Exec("DELETE FROM cache WHERE `key` = ?", key)
	if err != nil {
		return err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return ErrNotFound
	}
	return nil
}

func (m *mysql) DeleteAll() error {
	_, err := m.db.Exec("DELETE FROM cache")
	if err != nil {
		return err
	}
	return nil
}

func (m *mysql) Decrease(key string, count int) error {
	has, err := m.Has(key)
	if err != nil {
		return err
	}
	if !has {
		return ErrNotFound
	}
	_, err = m.db.Exec("UPDATE cache SET value = value - ? WHERE `key` = ?", count, key)
	if err != nil {
		return err
	}
	return nil
}

func (m *mysql) Increase(key string, count int) error {
	has, err := m.Has(key)
	if err != nil {
		return err
	}
	if !has {
		return ErrNotFound
	}
	_, err = m.db.Exec("UPDATE cache SET value = value + ? WHERE `key` = ?", count, key)
	if err != nil {
		return err
	}
	return nil
}

func (m *mysql) TTL(key string) (time.Duration, error) {
	i, err := m.get(key)
	if err != nil {
		return 0, err
	}
	return i.ttl - time.Since(i.createdAt), nil
}

func (m *mysql) get(key string) (item, error) {
	rows, err := m.db.Query("SELECT value, createdAt, ttl FROM cache WHERE `key` = ?", key)
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
		m.Delete(key)
		return item{}, ErrNotFound
	}
	return i, nil
}
