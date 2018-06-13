package main

import (
	"net/http"

	"github.com/go-mego/cache"
	"github.com/go-mego/mego"
)

func main() {
	e := mego.Default()
	e.Use(cache.New())
	e.GET("/", func(c *mego.Context, s cache.Store) {
		has, _ := s.Has("foobar")
		if !has {
			s.Set("foobar", "0")
		} else {
			s.Increase("foobar", 1)
		}
		v, _ := s.Get("foobar")
		c.String(http.StatusOK, v)
	})
	e.Run()
}
