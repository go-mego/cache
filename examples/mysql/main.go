package main

import (
	"net/http"

	"github.com/go-mego/cache"
	"github.com/go-mego/mego"
)

func main() {
	e := mego.Default()
	e.Use(cache.New(&cache.Options{
		Source:       cache.SourceTypeMySQL,
		SourceConfig: "root:root@/test",
	}))
	e.GET("/", func(c *mego.Context, s cache.Store) {
		has, err := s.Has("foobar")
		if err != nil {
			panic(err)
		}
		if !has {
			s.Set("foobar", "0")
		} else {
			s.Increase("foobar", 1)
		}
		v, err := s.Get("foobar")
		if err != nil {
			panic(err)
		}
		c.String(http.StatusOK, v)
	})
	e.Run()
}
