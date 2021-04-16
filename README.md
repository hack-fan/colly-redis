# Colly Redis Store
A redis store for colly use latest go-redis package.

Because the [official one](https://github.com/gocolly/redisstorage)
seems no longer being maintained.
It is hard to make a pull request for it,
since the new go-redis client will break the old api.

The logic code of this package is not change, thanks for
[gocolly/redisstorage](https://github.com/gocolly/redisstorage).

## Install

```
go get -u github.com/hack-fan/colly-redis
```


## Usage

```go
import (
	"github.com/gocolly/colly"
	"github.com/hack-fan/colly-redis"
	"github.com/go-redis/redis/v8"
)

// Make your any kind of go-redis client
rdb := redis.NewClient(&redis.Options{
    Addr:     "localhost:6379",
    Password: "", // no password set
    DB:       0,  // use default DB
})

storage := colly-redis.NewStorage(rdb)

c := colly.NewCollector()
err := c.SetStorage(storage)
if err != nil {
    panic(err)
}
```
