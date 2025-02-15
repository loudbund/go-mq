package client

import (
	"context"
	"fmt"
	protoMq "github.com/loudbund/go-mq/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func init() {
}

// 客户端实例
type client struct {
	Client protoMq.MqClient
}

// Pull 数据拉取
func (c *client) Pull(reqData *protoMq.PullDataReq, dataEvent func(res *protoMq.PullDataRes) bool) error {
	stream, _ := c.Client.PullData(context.Background(), reqData)
	for {
		if res, err := stream.Recv(); err != nil {
			if err.Error() == "EOF" {
				return nil
			}
			log.Error(err)
			return err
		} else {
			// 不继续接收则返回的是false，true则继续接收
			if !dataEvent(res) {
				return nil
			}
		}
	}
}

// NewClient 实例化客户端
func NewClient(Ip string, Port int) (*client, error) {
	c := &client{}
	fmt.Println("连接服务：", Ip, Port)

	dial, err := grpc.Dial(fmt.Sprintf("%s:%d", Ip, Port),
		grpc.WithInsecure(),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(10*1024*1024),
			grpc.MaxCallSendMsgSize(10*1024*1024),
		),
	)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	c.Client = protoMq.NewMqClient(dial)

	return c, nil
}
