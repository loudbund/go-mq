package service

import (
	"context"
	"fmt"
	protoMq "github.com/loudbund/go-mq/proto"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
)

type server struct {
	BoltDbControl *Controller
	protoMq.UnimplementedMqServer
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
	protoMq.RegisterMqServer(s, Server)

	log.Info(fmt.Sprintf("listen tcp %d", Port))
	listen, _ := net.Listen("tcp", fmt.Sprintf("%s:%d", Ip, +Port))
	s.Serve(listen)
}

// PushData 接收客户端写入的数据
func (s *server) PushData(ctx context.Context, req *protoMq.PushDataReq) (*protoMq.PushDataRes, error) {
	if err := s.BoltDbControl.WriteData(req.Channel, req.Data); err != nil {
		log.Error(err)
		return &protoMq.PushDataRes{ErrNum: 1, Msg: err.Error()}, nil
	}

	return &protoMq.PushDataRes{ErrNum: 0, Msg: ""}, nil
}

// PullData 响应客户端拉取数据
func (s *server) PullData(req *protoMq.PullDataReq, cliStr protoMq.Mq_PullDataServer) error {
	channels := lo.Associate(req.Channels, func(s string) (string, bool) { return s, true })
	Data := s.BoltDbControl.GetData(channels, req.Position, CfgBoltDb.DataRowCurPull)
	for _, v := range Data {
		if err := cliStr.Send(&protoMq.PullDataRes{
			ErrNum:   0,
			Position: v.Position,
			Channel:  v.Channel,
			Data:     v.Data,
		}); err != nil {
			return err
		}
	}
	return nil
}
