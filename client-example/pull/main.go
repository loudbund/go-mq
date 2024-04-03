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
			User:   "zhou",
			Ch:     "abc",
			BKType: true,
			BKName: "",
			BKKey:  "",
		}, func(res *protoMq.ResPullData) {
			// 3、处理收到的数据
			fmt.Println(res.BKName, res.BKKey, string(res.Data))
		}); err != nil {
			log.Panic(err)
		}
	}
}
