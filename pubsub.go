package pubsub

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/goibibo/mantle/backends"
	"github.com/goibibo/t-coredb"
	"sync"
)

type pusher chan string

const channelName = "go_background_processing"

var jobHandler map[string]pusher

var redisClient *mantle.RedisConn
var pscWrapper redis.PubSubConn
var rwMutex sync.RWMutex
var handlerLock sync.RWMutex

type Response struct {
	Id     string
	Result string
}

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
	pscWrapper.Subscribe(channelName)
}

func Subscribe(jobId string, channel pusher) {
	handlerLock.Lock()
	defer handlerLock.Unlock()
	jobHandler[jobId] = channel
}

func doPublish(chanName string, result string) {
	if chanName != channelName {
		panic(fmt.Sprintf("You are not subscribed to pubsub channel %s in go-pubsub", channelName))
		return
	}

	jobId, result := GetResult(result)
	channel, ok := jobHandler[jobId]
	if !ok {
		fmt.Printf("This job is not handled on this server, Ignore!")
	}

	channel <- result
}

func GetResult(result string) (string, string) {
	resp := &Response{}
	if err := json.Unmarshal([]byte(result), &resp); err != nil {
		fmt.Println("Bad response from RQ workers, Job ID", result)
		return result, ""
	}
	return resp.Id, resp.Result
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
