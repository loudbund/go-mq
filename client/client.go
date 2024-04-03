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

// 推送实例
type pushClient struct {
	putRes           protoMq.Mq_PushDataClient
	backSuccessOne   func(string, []byte, int)
	numSendButNoBack int
}

// PushOne 推送一条数据
func (p *pushClient) PushOne(pushChannel string, pushData []byte) {
	// 计算WaitRes
	p.numSendButNoBack++
	WaitRes := false
	if p.numSendButNoBack >= 2 {
		p.numSendButNoBack = 0
		WaitRes = true
	}

	// 写入一条数据
	if err := p.putRes.Send(&protoMq.ReqPushData{
		WaitRes: WaitRes,
		Ch:      pushChannel,
		Data:    pushData,
	}); err != nil {
		log.Panic(err)
	}
	// 接收一条反馈消息
	if WaitRes {
		// 接收回复消息
		if res, err := p.putRes.Recv(); err != nil {
			log.Panic(err)
		} else {
			p.backSuccessOne(pushChannel, pushData, (int)(res.ErrNum))
		}
	}
}

// 客户端实例
type client struct {
	Client protoMq.MqClient
}

// HandlePush 推数据句柄
func (c *client) HandlePush(backSuccessOne func(string, []byte, int)) (*pushClient, error) {
	p := &pushClient{numSendButNoBack: 0}
	if putRes, err := c.Client.PushData(context.Background()); err != nil {
		return nil, err
	} else {
		p.backSuccessOne = backSuccessOne
		p.putRes = putRes
		return p, nil
	}
}

// Pull 数据拉取
func (c *client) Pull(reqData *protoMq.ReqPullData, dataEvent func(res *protoMq.ResPullData)) error {
	stream, _ := c.Client.PullData(context.Background(), reqData)
	for {
		if res, err := stream.Recv(); err != nil {
			log.Error(err)
			return err
		} else {
			dataEvent(res)
		}
	}
}

// NewClient 实例化客户端
func NewClient(Ip string, Port int) (*client, error) {
	c := &client{}
	fmt.Println("连接服务：", Ip, Port)

	dial, err := grpc.Dial(fmt.Sprintf("%s:%d", Ip, Port), grpc.WithInsecure())
	if err != nil {
		log.Error(err)
		return nil, err
	}
	c.Client = protoMq.NewMqClient(dial)

	return c, nil
}
