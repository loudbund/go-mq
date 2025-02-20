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
	topics := [][]byte{
		[]byte("user"),
		//[]byte("homework"),
	}

	curPosition := (int64(1739178000) << 32) | int64(0)

	// 1、发送连接服务器
	if c, err := client.NewClient("127.0.0.1", 8090); err != nil {
		log.Panic(err)
	} else {

		// 2、发送数据拉取请求
		for {
			err = c.Pull(&protoMq.PullDataReq{Topics: topics, Position: curPosition},
				func(eData *protoMq.PullDataRes) bool {
					// 处理数据
					if curPosition != eData.Position {
						fmt.Println(string(eData.Topic), eData.Position, string(eData.Data))
					}

					// 游标更新
					curPosition = eData.Position

					return true
				},
			)
			if err != nil {
				log.Panic(err)
			}

			fmt.Println("waiting 1s")
			time.Sleep(time.Second * 1)
		}

	}

}
