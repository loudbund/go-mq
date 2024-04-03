package service

import (
	"errors"
	"fmt"
	protoMq "github.com/loudbund/go-mq/proto"
	"github.com/loudbund/go-utils/utils_v1"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"net"
	"time"
)

type server struct {
	protoMq.UnimplementedMqServer
}

// RunGrpcServer 启动服务端
func RunGrpcServer(Ip string, Port int) {
	s := grpc.NewServer()
	//注册事件
	protoMq.RegisterMqServer(s, &server{})

	log.Info(fmt.Sprintf("listen tcp %d", Port))
	listen, _ := net.Listen("tcp", fmt.Sprintf("%s:%d", Ip, +Port))
	s.Serve(listen)
}

// PushData 接收客户端写入的数据
// 先记录到变量里，当参数WaitRes为true时，才开始写入存储，并回发一条消息
// 如果连续5000条数据都没有收到WaitRes为true的消息，则报错发送错误给客户端
// 错误码:
// 10: 单次PushData 各数据里的CH名称不一致
// 20: 单次PushData 数据条数超过cfgBoltDb.pushPerNumLimit
// 30: 写入存储失败
func (s *server) PushData(cliStr protoMq.Mq_PushDataServer) error {
	fmt.Println(utils_v1.Time().DateTime(), "welcome to push data ")
	for {
		// 临时存储频道名和数据的变量
		ChannelName, Data := "", make([][]byte, 0)

		// 循环接收一批数据
		for {
			if item, err := cliStr.Recv(); err == nil {
				if ChannelName == "" {
					ChannelName = item.Ch
				}
				// 一批数据频道名不一致发送错误
				if ChannelName != item.Ch {
					err := errors.New("单次PushData 各数据里的CH名称不一致")
					log.Error(err)

					if err := cliStr.Send(&protoMq.ResPushData{
						ErrNum: 10,
					}); err != nil {
						log.Error("push Data send error", err)
						break
					}
					return err
				}
				// 缓存数据
				Data = append(Data, item.Data)
				if item.WaitRes {
					//fmt.Println("rev WaitRes")
					break
				}

				// 一批数据条数过多，则报错
				if len(Data) > 5000 {
					err := errors.New("单次PushData 数据条数超过5000条")
					log.Error(err)

					if err := cliStr.Send(&protoMq.ResPushData{
						ErrNum: 20,
					}); err != nil {
						log.Error("push Data send error", err)
						break
					}
					return err
				}
			} else {
				log.Error("break, err :", err)
				return err
			}
		}

		// 写入数据到存储
		if err := (&Controller{}).PushWriteData(ChannelName, Data); err != nil {
			log.Error(err)

			if err := cliStr.Send(&protoMq.ResPushData{
				ErrNum: 30,
			}); err != nil {
				log.Error("push Data send error:", err)
			}

			return err
		}

		// 发送结果
		if err := cliStr.Send(&protoMq.ResPushData{
			ErrNum: 0,
		}); err != nil {
			log.Error("push Data send error:", err)
			return err
		}
	}
}

// PullData 响应客户端拉取数据
// 错误码:
// 10: 设置存储位置失败
// 20: 读取数据失败
func (s *server) PullData(req *protoMq.ReqPullData, cliStr protoMq.Mq_PullDataServer) error {
	fmt.Println(utils_v1.Time().DateTime(), "welcome to pull data ")
	// 位置参数处理, false:使用远端存储的位置 true:使用传递的参数
	if req.BKType {
		if err := (&Controller{}).PullSetChannelPosition(req.User, req.Ch, req.BKName, req.BKKey); err != nil {
			if err := cliStr.Send(&protoMq.ResPullData{
				ErrNum: 10,
				Data:   []byte(err.Error()),
				BKName: "",
				BKKey:  "",
			}); err != nil {
				log.Println("break, err :", err)
				return err
			}
		}
	}

	// 循环发送数据
	for {
		lastBucketName, lastBucketKey := "", ""
		if Data, retBucketName, retBucketKey, err := (&Controller{}).PullGetData(req.User, req.Ch); err != nil {
			log.Error(err)
			if err := cliStr.Send(&protoMq.ResPullData{
				ErrNum: 20,
				Data:   []byte(err.Error()),
				BKName: "",
				BKKey:  "",
			}); err != nil {
				log.Error(err)
				return err
			}
			return err
		} else {
			for k, v := range Data {
				// 计算将要发送的bucket信息，只有最后一条数据才带bucket信息，其他的数据bucket信息都用空字串
				BkName, BkKey := "", ""
				if k == len(Data)-1 {
					BkName, BkKey = retBucketName, retBucketKey
				}
				//log.Info(111, BkName, BkKey, v)

				// 发送数据
				if err := cliStr.Send(&protoMq.ResPullData{
					ErrNum: 0,
					Data:   []byte(v),
					BKName: BkName,
					BKKey:  BkKey,
				}); err != nil {
					log.Println("break, err :", err)
					return err
				}
			}
			// 刷新用户消息拉取位置
			if lastBucketName != retBucketName || lastBucketKey != retBucketKey {
				if err := (&Controller{}).PullSetChannelPosition(req.User, req.Ch, retBucketName, retBucketKey); err != nil {
					log.Error(err)
					return err
				}
				lastBucketName, lastBucketKey = retBucketName, retBucketKey
			}
		}
		time.Sleep(1 * time.Second)
	}
}
