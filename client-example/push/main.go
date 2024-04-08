package main

import (
	"fmt"
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
		// 2、建立数据推送通道
		if putRes, err := c.HandlePush(func(channel string, successData []byte, ErrNum int) {
			if ErrNum != 0 {
				log.Error(fmt.Sprintf("ErrNum:%d\n", ErrNum))
			}
			// 3、在此处理回调消息
			// fmt.Println("success Data", ErrNum)
		}); err != nil {
			log.Panic(err)
		} else {
			// 4、在此发送数据
			for {
				data := "data:" + utils_v1.Time().DateTime()
				log.Info(data)
				putRes.PushOne("abc", []byte(data))
				time.Sleep(time.Second * 1)
			}
		}
	}
}
