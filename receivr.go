package rqrcvr

import (
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/goibibo/mantle/backends"
	"github.com/goibibo/t-coredb"
	"sync"
)

//At the moment the pubsub channel on which servers expect result is hardcoded
const defaultChan = "go_background_processing"

//This is the channel where response from RQ work is written
type pusher chan string

//This map maps job_id to channel mapping
var jobHandler map[string]pusher

//We are using one redis client solely for handling pubsub thingy
var redisClient *mantle.RedisConn

//This wraps redis for pubsub functionality
var pscWrapper redis.PubSubConn

//saving these variables from concurrent accesses
var rwMutex sync.RWMutex
var handlerLock sync.RWMutex

//Id and Result are read in json.Unmarshall => made public
type Response struct {
	Id     string
	Result string
}

//This should be explicitly called before using this module
func InitClient(vertical string) {
	//protect two guys trying to read rqRedisPool at once
	rwMutex.Lock()
	defer rwMutex.Unlock()

	//configure redisClient
	if redisClient == nil {
		redisClient, err := db.PureRedisClientFor(vertical)
		if err != nil {
			panic(err)
		}
		pscWrapper = redis.PubSubConn{redisClient.Conn}
	}

	//configure jobHandler if it doesnt exist
	if jobHandler == nil {
		jobHandler = make(map[string]pusher)
	}

	//subscribe to the pubsub channel
	pscWrapper.Subscribe(defaultChan)
}

//Once you enqueue a job to RQ worker using github.com/vireshas/rq
//you create a channel and subscribe to pubsub for the response
//Every job in RQ is identified by a unique identifier
func Subscribe(jobId string, channel pusher) {
	handlerLock.Lock()
	defer handlerLock.Unlock()
	jobHandler[jobId] = channel
}

//Checks if the job is from produced from this server
//if it is enqueued from the present server, the result is written to the channel on which it is hearing for result
func writeToChannel(chanName string, result string) {
	//TODO: right now the redis pubsub channel is hardcoded
	//if we can remove that dependency we can make this a generic module
	if chanName != defaultChan {
		errMsg := fmt.Sprintf("You are not subscribed to channel %s in gorq-rcvr", defaultChan)
		panic(errMsg)
	}

	jobId, result := parseResult(result)
	//check if this server enqueued this job
	//if so then get the channel from jobHandler and write the response to it
	channel, ok := jobHandler[jobId]
	if !ok {
		fmt.Printf("This job is not handled on this server, Ignore!")
		return
	}

	channel <- result
}

//Response from RQ workers would be a json
//Json structure is something like this
//Response = {"id":job_id, "result": response_from_a_worker}
func parseResult(result string) (string, string) {
	resp := &Response{}
	if err := json.Unmarshal([]byte(result), &resp); err != nil {
		fmt.Println("Bad response from RQ workers, Job ID", result)
		return result, ""
	}
	return resp.Id, resp.Result
}

//Publisher is a deamon which silently listens to messages that are coming in
//You need to explicitly start this deamon
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
