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

// DataEvent 响应回调数据结构
type DataEvent struct {
	Position int64
	Topic    string
	Data     []byte
}

// 客户端实例
type client struct {
	Client protoMq.MqClient
}

// Pull 数据拉取
func (c *client) Pull(reqData *protoMq.PullDataReq, callBack func(res *protoMq.PullDataRes) bool) error {
	stream, _ := c.Client.PullData(context.Background(), reqData)

	for {
		// 取数据
		res, err := stream.Recv()
		if err != nil {

			if err.Error() != "EOF" {
				log.Error(err)
				return err
			}

			return nil
		}

		// 回调
		if !callBack(res) {
			return nil
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
