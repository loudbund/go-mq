package client

import (
	"context"
	"fmt"
	protoMq "github.com/loudbund/go-mq/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"sync"
	"time"
)

func init() {
}

// 推送实例
type pushClient struct {
	putRes           protoMq.Mq_PushDataClient
	backSuccessOne   func(string, []byte, int)
	numSendButNoBack int
	timerTimeOut     *time.Timer
	locker           sync.RWMutex
}

// PushOne 推送一条数据
func (p *pushClient) PushOne(pushChannel string, pushData []byte) {
	p.locker.Lock()
	defer p.locker.Unlock()

	// 计算DataType
	p.numSendButNoBack++
	DataType := protoMq.DataType_DataOnly
	if p.numSendButNoBack >= 200 {
		p.numSendButNoBack = 0
		DataType = protoMq.DataType_DataEnd

		// 停掉定时器
		if p.timerTimeOut != nil {
			p.timerTimeOut.Stop()
			p.timerTimeOut = nil
		}
	}

	// 写入一条数据
	if err := p.putRes.Send(&protoMq.ReqPushData{
		DataType: DataType,
		Ch:       pushChannel,
		Data:     pushData,
	}); err != nil {
		log.Panic(err)
	}
	// 接收一条反馈消息
	if DataType == protoMq.DataType_DataEnd {
		// 接收回复消息
		if res, err := p.putRes.Recv(); err != nil {
			log.Panic(err)
		} else {
			p.backSuccessOne(pushChannel, pushData, (int)(res.ErrNum))
		}
	}
	// 有数据但是没有定时器，则添加定时器
	if p.numSendButNoBack > 0 && p.timerTimeOut == nil {
		p.timerTimeOut = time.AfterFunc(time.Second*1, func() {
			p.locker.Lock()
			defer func() { p.timerTimeOut = nil; p.locker.Unlock() }()

			// 写入一条空数据
			if err := p.putRes.Send(&protoMq.ReqPushData{
				DataType: protoMq.DataType_EndOnly,
				Ch:       pushChannel,
				Data:     make([]byte, 0),
			}); err != nil {
				log.Panic(err)
			}
			// 接收一条反馈消息
			if res, err := p.putRes.Recv(); err != nil {
				log.Panic(err)
			} else {
				p.backSuccessOne(pushChannel, pushData, (int)(res.ErrNum))
			}
		})
	}
}

// 客户端实例
type client struct {
	Client protoMq.MqClient
}

// HandlePush 推数据句柄
func (c *client) HandlePush(backSuccessOne func(string, []byte, int)) (*pushClient, error) {
	p := &pushClient{numSendButNoBack: 0, timerTimeOut: nil}
	if putRes, err := c.Client.PushData(context.Background()); err != nil {
		return nil, err
	} else {
		p.backSuccessOne = backSuccessOne
		p.putRes = putRes
		return p, nil
	}
}

// Pull 数据拉取
func (c *client) Pull(reqData *protoMq.ReqPullData, dataEvent func(res *protoMq.ResPullData) bool) error {
	stream, _ := c.Client.PullData(context.Background(), reqData)
	for {
		if res, err := stream.Recv(); err != nil {
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

	dial, err := grpc.Dial(fmt.Sprintf("%s:%d", Ip, Port), grpc.WithInsecure())
	if err != nil {
		log.Error(err)
		return nil, err
	}
	c.Client = protoMq.NewMqClient(dial)

	return c, nil
}
