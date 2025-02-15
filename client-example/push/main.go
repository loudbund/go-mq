package main

import (
	"context"
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
	// 1、连接服务器
	if c, err := client.NewClient("127.0.0.1", 8090); err != nil {
		log.Panic(err)
	} else {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		if r, err := c.Client.PushData(ctx, &protoMq.PushDataReq{
			Channel: "user",
			Data:    []byte("hello" + utils_v1.Time().DateTime()),
		}); err != nil {
			log.Panic(err)
		} else {
			log.Info(r.ErrNum, r.Msg)
		}
	}
}
