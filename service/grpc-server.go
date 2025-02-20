package service

import (
	"context"
	"fmt"
	"github.com/loudbund/go-mq/client"
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
	if err := s.BoltDbControl.WriteData(req.Topic, req.Data); err != nil {
		log.Error(err)
		return &protoMq.PushDataRes{ErrNum: 1, Msg: err.Error()}, nil
	}

	return &protoMq.PushDataRes{ErrNum: 0, Msg: ""}, nil
}

// PullData 响应客户端拉取数据
func (s *server) PullData(req *protoMq.PullDataReq, cliStr protoMq.Mq_PullDataServer) error {
	// 构建主题映射
	reqTopicMap := lo.Associate(req.Topics, func(topic string) (string, bool) { return topic, true })

	cursorBucket, cursorDataId := int32(req.Position>>32), int32(req.Position)
	for i := 0; i < 500; i++ {
		// 从 BoltDB 获取数据
		topicId2Name, DataGroup := s.BoltDbControl.GetData(reqTopicMap, cursorBucket, cursorDataId, CfgBoltDb.DataRowCurPull)
		cursorBucket, cursorDataId = DataGroup.Bucket, DataGroup.Items[len(DataGroup.Items)-1].DataId

		// 发送数据给客户端
		resp := &protoMq.PullDataRes{
			ErrNum:   0,
			TopicMap: topicId2Name,
			DataMix:  client.DataGroupEncode(req.ZLib, DataGroup),
		}
		if err := cliStr.Send(resp); err != nil {
			log.Errorf("发送数据出错：%s", err.Error())
			return err
		}

		// 没有取到数据了，退出本轮请求
		if DataGroup.Items[0].TopicId == 0 {
			break
		}
	}

	return nil
}
