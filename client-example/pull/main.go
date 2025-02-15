package main

import (
	"fmt"
	"github.com/loudbund/go-mq/client"
	protoMq "github.com/loudbund/go-mq/proto"
	log "github.com/sirupsen/logrus"
	"time"
)

func init() {
	log.SetReportCaller(true)
}

// 6、主函数 -------------------------------------------------------------------------
func main() {
	channels := []string{"user"}
	position := (int64(1739178000) << 32) | int64(0)
	// 1、发送连接服务器
	if c, err := client.NewClient("127.0.0.1", 8090); err != nil {
		log.Panic(err)
	} else {
		// 2、发送数据拉取请求
		for {
			getFinish := false
			if err := c.Pull(&protoMq.PullDataReq{Channels: channels, Position: position},
				func(res *protoMq.PullDataRes) bool {
					// 远端出现错误
					if res.ErrNum != 0 {
						log.Errorf("接收数据过程中出错，错误码：%d", res.ErrNum)
						return false
					}

					// 刷新已接收位置
					position = res.Position

					// 接收到空数据，表示已经接收完所有数据
					if res.Channel == "" {
						getFinish = true
						return false
					}

					// 处理接收到的数据
					fmt.Println(res.Channel, res.Position, string(res.Data))

					// 返回true表示继续接收数据
					return true
				}); err != nil {
				log.Panic(err)
			}

			// 接收完成后等待1秒继续接收
			if getFinish == true {
				fmt.Println("sleep 1 s")
				time.Sleep(time.Second * 1)
			}
		}
	}
}
