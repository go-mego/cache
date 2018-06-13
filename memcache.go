package cache

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	mem "github.com/bradfitz/gomemcache/memcache"
)

func newMemcache(options *Options) Store {
	db := mem.New(strings.Split(options.SourceConfig, ";")...)
	return &memcache{
		db: db,
	}
}

type memcache struct {
	db *mem.Client
}

func (m *memcache) Set(key string, value string, options ...*SetOptions) error {
	ttl := time.Duration(-1)
	if len(options) > 0 {
		ttl = options[0].TTL
	}
	item := item{
		value:     value,
		createdAt: time.Now(),
		ttl:       ttl,
	}
	v, err := json.Marshal(item)
	if err != nil {
		return err
	}
	return m.db.Set(&mem.Item{
		Key:   key,
		Value: v,
	})
}

func (m *memcache) Get(key string) (string, error) {
	i, err := m.get(key)
	if err != nil {
		return "", err
	}
	return i.value, nil
}

func (m *memcache) Has(key string) (bool, error) {
	_, err := m.get(key)
	if err != nil {
		if err == ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (m *memcache) Delete(key string) error {
	err := m.db.Delete(key)
	if err != nil {
		if err == mem.ErrCacheMiss {
			return ErrNotFound
		}
		return err
	}
	return nil
}

func (m *memcache) DeleteAll() error {
	return m.db.DeleteAll()
}

func (m *memcache) Decrease(key string, count int) error {
	i, err := m.get(key)
	if err != nil {
		return err
	}
	intV, err := strconv.Atoi(i.value)
	if err != nil {
		return err
	}
	i.value = strconv.Itoa(intV - count)
	newV, err := json.Marshal(i)
	if err != nil {
		return err
	}
	err = m.db.Replace(&mem.Item{
		Key:   key,
		Value: newV,
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *memcache) Increase(key string, count int) error {
	i, err := m.get(key)
	if err != nil {
		return err
	}
	intV, err := strconv.Atoi(i.value)
	if err != nil {
		return err
	}
	i.value = strconv.Itoa(intV + count)
	newV, err := json.Marshal(i)
	if err != nil {
		return err
	}
	err = m.db.Set(&mem.Item{
		Key:   key,
		Value: newV,
	})
	if err != nil {
		return err
	}
	return nil
}

func (m *memcache) TTL(key string) (time.Duration, error) {
	i, err := m.get(key)
	if err != nil {
		return -1, err
	}
	return i.ttl - time.Since(i.createdAt), nil
}

func (m *memcache) get(key string) (item, error) {
	v, err := m.db.Get(key)
	if err != nil {
		if err == mem.ErrCacheMiss {
			return item{}, ErrNotFound
		}
		return item{}, err
	}
	var i item
	err = json.Unmarshal(v.Value, &i)
	if err != nil {
		return item{}, err
	}
	if time.Since(i.createdAt) > i.ttl {
		m.db.Delete(key)
		return item{}, ErrNotFound
	}
	return i, nil
}
