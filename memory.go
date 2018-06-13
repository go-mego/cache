package cache

import (
	"strconv"
	"time"
)

func newMemory(options *Options) Store {
	return &memory{
		db: make(map[string]item),
	}
}

type memory struct {
	db map[string]item
}

func (m *memory) Set(key string, value string, options ...*SetOptions) error {
	ttl := time.Duration(-1)
	if len(options) > 0 {
		ttl = options[0].TTL
	}
	m.db[key] = item{
		value:     value,
		createdAt: time.Now(),
		ttl:       ttl,
	}
	return nil
}

func (m *memory) Get(key string) (string, error) {
	v, ok := m.db[key]
	if !ok {
		return "", ErrNotFound
	}
	if time.Since(v.createdAt) > v.ttl {
		delete(m.db, key)
		return "", ErrNotFound
	}
	return v.value, nil
}

func (m *memory) Has(key string) (bool, error) {
	v, _ := m.db[key]
	if time.Since(v.createdAt) > v.ttl {
		delete(m.db, key)
		return false, nil
	}
	return true, nil
}

func (m *memory) Delete(key string) error {
	has, err := m.Has(key)
	if err != nil {
		return err
	}
	if !has {
		return ErrNotFound
	}
	delete(m.db, key)
	return nil
}

func (m *memory) DeleteAll() error {
	m.db = make(map[string]item)
	return nil
}

func (m *memory) Decrease(key string, count int) error {
	v, err := m.Get(key)
	if err != nil {
		return err
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return err
	}
	v = strconv.Itoa(i - count)
	return m.Set(key, v)
}

func (m *memory) Increase(key string, count int) error {
	v, err := m.Get(key)
	if err != nil {
		return err
	}
	i, err := strconv.Atoi(v)
	if err != nil {
		return err
	}
	v = strconv.Itoa(i + count)
	return m.Set(key, v)
}

func (m *memory) TTL(key string) (time.Duration, error) {
	has, err := m.Has(key)
	if err != nil {
		return -1, err
	}
	if !has {
		return -1, ErrNotFound
	}
	v, _ := m.db[key]
	return v.ttl - time.Since(v.createdAt), nil
}
