package main

import (
	"fmt"
	mqV1 "github.com/loudbund/go-mq/api/v1"
	"github.com/loudbund/go-mq/client"
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
			n, err := c.Pull(&mqV1.PullDataReq{
				VersionClient: client.VersionClient,
				Topics:        topics,
				Position:      curPosition,
				Username:      client.PasswordEncode("root"),
				Password:      client.PasswordEncode("test1234test"),
			},
				func(eData *mqV1.PullDataRes) bool {
					// 处理数据
					if curPosition != eData.Position {
						fmt.Println(string(eData.Topic), eData.Position)
						if len(eData.Topic) > 0 {
							Data := &struct {
								Name  string
								Value string
							}{}
							if err := client.Decode(eData.Data, Data); err != nil {
								log.Error(err)
							}
							fmt.Println(len(eData.Data), *Data)
						}
					}

					// 游标更新
					curPosition = eData.Position

					return true
				},
			)

			if err != nil {
				log.Panic(err)
			}

			if n == 0 {
				fmt.Println("waiting 1s")
				time.Sleep(time.Second * 1)
			}
		}

	}

}
