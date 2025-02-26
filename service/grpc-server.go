package service

import (
	"context"
	"fmt"
	mqV1 "github.com/loudbund/go-mq/api/v1"
	"github.com/loudbund/go-mq/client"
	"github.com/loudbund/go-mq/config"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"time"
)

type server struct {
	BoltDbControl *Controller
	mqV1.UnimplementedMqServer
}

// RunGrpcServer 启动服务端
func RunGrpcServer(Ip string, Port int) {
	Server := &server{}

	// boltDb初始化
	Server.BoltDbControl = &Controller{}
	Server.BoltDbControl.Init()

	// grpc
	s := grpc.NewServer()

	//注册事件
	mqV1.RegisterMqServer(s, Server)

	log.Info(fmt.Sprintf("listen tcp %d", Port))
	listen, _ := net.Listen("tcp", fmt.Sprintf("%s:%d", Ip, +Port))
	s.Serve(listen)
}

// PushData 接收客户端写入的数据
func (s *server) PushData(ctx context.Context, req *mqV1.PushDataReq) (*mqV1.PushDataRes, error) {

	err := s.BoltDbControl.WriteData(TTopicName(req.Topic), req.Data)

	if err != nil {
		log.Error(err)
		return &mqV1.PushDataRes{ErrNum: 1, Msg: err.Error()}, nil
	}

	return &mqV1.PushDataRes{ErrNum: 0, Msg: ""}, nil
}

// PullData 响应客户端拉取数据
func (s *server) PullData(req *mqV1.PullDataReq, cliStr mqV1.Mq_PullDataServer) error {
	// 权限校验
	if client.PasswordEncode(config.CfgServer.Username) != req.Username ||
		client.PasswordEncode(config.CfgServer.Password) != req.Password {
		log.Error("拉取数据账号密码校验失败")
		return fmt.Errorf("拉取数据账号密码校验失败")
	}

	// 构建频道映射
	reqTopicMap := map[TTopicName]bool{}
	for _, v := range req.Topics {
		reqTopicMap[TTopicName(v)] = true
	}

	// 提取出当前的 bucket 和 dataId
	curBucket, curDataId := TBucketId(req.Position>>32), TDataId(req.Position)

	// 循环取数据
	for i := 0; i < config.CfgServer.DataRowPerPull; i++ {
		// 从 BoltDB 获取数据
		topic, bucket, dataId, data := s.BoltDbControl.GetData(reqTopicMap, curBucket, curDataId)

		// 发送数据给客户端
		resp := &mqV1.PullDataRes{
			ErrNum:   0,
			Position: int64(bucket)<<32 | int64(dataId),
			Topic:    []byte(topic),
			Data:     data,
		}
		if err := cliStr.Send(resp); err != nil {
			log.Errorf("发送数据出错：%s", err.Error())
			return err
		}

		// 没有取到数据了，退出本轮请求; 延时下避免客户端频繁请求
		if topic == "" {
			time.Sleep(time.Millisecond * 100)
			break
		}

		// 更新游标
		curBucket, curDataId = bucket, dataId
	}

	return nil
}
