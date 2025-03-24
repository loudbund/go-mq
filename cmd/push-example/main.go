package main

import (
	"fmt"
	mqV1 "github.com/loudbund/go-mq/api/v1"
	"github.com/loudbund/go-mq/client"
	"github.com/loudbund/go-utils/utils_v1"
	log "github.com/sirupsen/logrus"
	"time"
)

func init() {
	log.SetReportCaller(true)
}

// 6、主函数 -------------------------------------------------------------------------
func main() {
	// 1、连接服务器
	if c, err := client.NewClient("127.0.0.1", 8090); err != nil {
		log.Panic(err)
	} else {
		D := &mqV1.PushDataReq{
			VersionClient: client.VersionClient,
			//Topic: [][]byte{[]byte("user"), []byte("homework")}[rand.Intn(2)],
			Topic: []byte("user"),
			Data: client.Encode(struct {
				Name  string
				Value string
			}{Name: "hello333333333333", Value: utils_v1.Time().DateTime()}, true),
		}
		fmt.Println(D)

		// 2、发送消息
		for {
			if err = c.Push(D); err != nil {
				log.Error(err)
				log.Info("延时5秒后重试")
				time.Sleep(time.Second * 5)
			} else {
				log.Info("发送成功")
				break
			}
		}
	}
}
