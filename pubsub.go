package main

import (
	"bytes"
	"fmt"
	"github.com/garyburd/redigo/redis"
	. "github.com/kisielk/og-rek"
)

func main() {
	//encoding stuff
	p := &bytes.Buffer{}
	e := NewEncoder(p)
	f := []interface{}{"add.add", nil, []interface{}{2, 3}, map[string]string{"pubsub": "true"}}
	e.Encode(f)
	fmt.Println("encoded value", string(p.Bytes()))

	job := make(map[string]string)
	job_id := "231"

	//pushing encoded value in redis
	c, err := redis.Dial("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	defer c.Close()

	pubsub, err := redis.Dial("tcp", ":6379")
	if err != nil {
		panic(err)
	}
	defer pubsub.Close()

	job["data"] = string(p.Bytes())
	queue_id := "rq:job:" + job_id

	_, err = c.Do("HMSET", redis.Args{queue_id}.AddFlat(job)...)
	if err != nil {
		fmt.Println("HMSET", err)
	}

	psc := redis.PubSubConn{pubsub}
	psc.Subscribe(job_id)
	psc.Subscribe("111")

	_, err = c.Do("RPUSH", "rq:queue:default", job_id)
	if err != nil {
		fmt.Println("RPUSH", err)
	}

	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			fmt.Printf("%s: message: %s\n", v.Channel, v.Data)
			break
		case redis.Subscription:
			fmt.Printf("%s: %s %d\n", v.Channel, v.Kind, v.Count)
		case error:
			fmt.Printf("error")
		}
	}
}
