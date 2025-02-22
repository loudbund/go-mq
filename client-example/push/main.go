package main

import (
	"fmt"
	"github.com/loudbund/go-mq/client"
	protoMq "github.com/loudbund/go-mq/proto"
	"github.com/loudbund/go-utils/utils_v1"
	log "github.com/sirupsen/logrus"
	"math/rand"
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
		D := &protoMq.PushDataReq{
			Topic: [][]byte{[]byte("user"), []byte("homework")}[rand.Intn(2)],
			Data:  []byte(`{"hello":"` + utils_v1.Time().DateTime() + `"}`),
		}
		fmt.Println(D)

		if err = c.Push(D); err != nil {
			log.Error(err)
		}
	}
}
