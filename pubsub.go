package pubsub

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/goibibo/mantle/backends"
	"github.com/goibibo/t-coredb"
	"sync"
)

type pusher chan string

var jobHandler map[string]pusher
var redisClient *mantle.RedisConn
var pscWrapper redis.PubSubConn
var rwMutex sync.RWMutex
var handlerLock sync.RWMutex

func InitClient(vertical string) {
	//protect two guys trying to read rqRedisPool at once
	rwMutex.Lock()
	defer rwMutex.Unlock()
	if redisClient == nil {
		redisClient, err := db.PureRedisClientFor(vertical)
		if err != nil {
			panic(err)
		}
		pscWrapper = redis.PubSubConn{redisClient.Conn}
	}
	if jobHandler == nil {
		jobHandler = make(map[string]pusher)
	}
}

func Subscribe(jobId string, chanName pusher) {
	handlerLock.Lock()
	defer handlerLock.Unlock()
	jobHandler[jobId] = chanName
	pscWrapper.Subscribe(jobId)
}

func doPublish(jobId string, result string) {
	channel, ok := jobHandler[jobId]
	if !ok {
		panic("No channel to push result")
	}
	channel <- result
}

func StartPublisher() {
	for {
		switch v := pscWrapper.Receive().(type) {
		case redis.Message:
			fmt.Printf("%s: message: %s\n", v.Channel, v.Data)
			doPublish(string(v.Channel), string(v.Data))
		case redis.Subscription:
			fmt.Printf("%s: %s %d\n", v.Channel, v.Kind, v.Count)
		case error:
			fmt.Printf("error")
		}
	}
}
