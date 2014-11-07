        package main

        import (
                "fmt"
                "github.com/vireshas/go-pubsub"
        )

        func main() {
                out := make(chan string)
                InitPubsubClient("r2", out)
                Subscribe("1010")
                go Publish()
                fmt.Println(<-out)
        }
