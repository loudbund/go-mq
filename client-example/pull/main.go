package main

import (
	"fmt"
	"github.com/loudbund/go-mq/client"
	protoMq "github.com/loudbund/go-mq/proto"
	log "github.com/sirupsen/logrus"
)

func init() {
	log.SetReportCaller(true)
}

// 6、主函数 -------------------------------------------------------------------------
func main() {
	// 1、发送连接服务器
	if c, err := client.NewClient("127.0.0.1", 8090); err != nil {
		log.Panic(err)
	} else {
		// 2、发送数据拉取请求
		if err := c.Pull(&protoMq.ReqPullData{
			User: "zhou",
			Ch:   "abc",
			//BKType: protoMq.BKType_RemoteDefaultAll,
			BKType: protoMq.BKType_UserSet,
			BKName: "0",
			BKKey:  "0",
		}, func(res *protoMq.ResPullData) {
			// 3、处理收到的数据
			if res.ErrNum != 0 {
				log.Error(res)
			}
			fmt.Println(string(res.Data), res.BKName, res.BKKey)
		}); err != nil {
			log.Panic(err)
		}
	}
}
