package rqrcvr

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/goibibo/mantle/backends"
	"github.com/goibibo/t-coredb"
	"sync"
)

const defaultChan = "go_background_processing"

var jobHandler map[string]pusher
var redisClient *mantle.RedisConn
var pscWrapper redis.PubSubConn
var rwMutex sync.RWMutex
var handlerLock sync.RWMutex

type pusher chan string
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
	pscWrapper.Subscribe(defaultChan)
}

func Subscribe(jobId string, channel pusher) {
	handlerLock.Lock()
	defer handlerLock.Unlock()
	jobHandler[jobId] = channel
}

func writeToChannel(chanName string, result string) {
	if chanName != defaultChan {
		errMsg := fmt.Sprintf("You are not subscribed to channel %s in gorq-rcvr", defaultChan)
		panic(errMsg)
	}

	jobId, result := parseResult(result)
	channel, ok := jobHandler[jobId]
	if !ok {
		fmt.Printf("This job is not handled on this server, Ignore!")
		return
	}

	channel <- result
}

func parseResult(result string) (string, string) {
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
			writeToChannel(string(v.Channel), string(v.Data))
		case redis.Subscription:
			fmt.Printf("%s: %s %d\n", v.Channel, v.Kind, v.Count)
		case error:
			fmt.Printf("error")
		}
	}
}
