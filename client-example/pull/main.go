package main

import (
	"fmt"
	"github.com/loudbund/go-mq/client"
	protoMq "github.com/loudbund/go-mq/proto"
	"github.com/loudbund/go-utils/utils_v1"
	log "github.com/sirupsen/logrus"
	"time"
)

func init() {
	log.SetReportCaller(true)
}

// 6、主函数 -------------------------------------------------------------------------
func main() {
	topics := []string{
		"user",
		//"homework",
	}

	curPosition := (int64(1739178000) << 32) | int64(0)

	// 1、发送连接服务器
	if c, err := client.NewClient("127.0.0.1", 8090); err != nil {
		log.Panic(err)
	} else {

		// 2、发送数据拉取请求
		for {
			wait := false

			if err := c.Pull(&protoMq.PullDataReq{Topics: topics, Position: curPosition, ZLib: true},
				func(eventData *client.DataEvent) bool {
					// 收到有效数据
					if eventData.Topic != "" {
						fmt.Println(eventData.Topic, eventData.Position, string(eventData.Data))
					}

					// 游标更新
					curPosition = eventData.Position

					// 返回true表示继续接收数据
					if eventData.Topic == "" {
						wait = true
						return false
					}

					return true
				}); err != nil {
				log.Panic(err)
			}

			if wait {
				fmt.Println("wait", utils_v1.Time().DateTime())
				time.Sleep(time.Second)
			}
		}

	}

}
