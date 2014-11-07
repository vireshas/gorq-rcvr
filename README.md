        package main

        import (
                "fmt"
                "github.com/goibibo/t-settings"
                "github.com/vireshas/go-pubsub"
        )

        func main() {
                settings.Configure()
                out := make(chan string)
                pubsub.InitClient("r2")
                pubsub.Subscribe("1010", out)
                go pubsub.Publish()
                fmt.Println(<-out)
        }
