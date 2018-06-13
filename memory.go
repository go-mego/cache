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
	ttl := time.Duration(0)
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
	i, err := m.get(key)
	if err != nil {
		return "", err
	}
	return i.value, nil
}

func (m *memory) Has(key string) (bool, error) {
	_, err := m.get(key)
	if err != nil {
		if err == ErrNotFound {
			return false, nil
		}
		return false, err
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
	i, err := m.get(key)
	if err != nil {
		return err
	}
	v, err := strconv.Atoi(i.value)
	if err != nil {
		return err
	}
	i.value = strconv.Itoa(v - count)
	m.db[key] = i
	return nil
}

func (m *memory) Increase(key string, count int) error {
	i, err := m.get(key)
	if err != nil {
		return err
	}
	v, err := strconv.Atoi(i.value)
	if err != nil {
		return err
	}
	i.value = strconv.Itoa(v + count)
	m.db[key] = i
	return nil
}

func (m *memory) TTL(key string) (time.Duration, error) {
	i, err := m.get(key)
	if err != nil {
		return 0, err
	}
	return i.ttl - time.Since(i.createdAt), nil
}

func (m *memory) get(key string) (item, error) {
	v, ok := m.db[key]
	if !ok {
		return item{}, ErrNotFound
	}
	if v.ttl != 0 && time.Since(v.createdAt) > v.ttl {
		delete(m.db, key)
		return item{}, ErrNotFound
	}
	return v, nil
}
