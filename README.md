####Client which receives results from gorq workers via Redis#pubsub.

        package main

        import (
                "fmt"
                "github.com/goibibo/t-settings"
                "github.com/vireshas/gorq-rcvr"
        )

        func main() {
                settings.Configure()
                out := make(chan string)
                rqrcvr.InitClient("r2")
                rqrcvr.Subscribe("1010", out)
                go rqrcvr.Publish()
                fmt.Println(<-out)
        }


open *redis-cli*  
publish go_background_processing '{"id": "1010", "result": "helloworld"}'
