package client

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/gob"
	"fmt"
	protoMq "github.com/loudbund/go-mq/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"io"
)

func init() {
}

// DataEvent 响应回调数据结构
type DataEvent struct {
	Position int64  // 数据位置
	Topic    string // 数据频道
	Data     []byte // 数据内容
}

// 客户端实例
type client struct {
	Client protoMq.MqClient
}

// Pull 数据拉取
func (c *client) Pull(reqData *protoMq.PullDataReq, callBack func(res *protoMq.PullDataRes) bool) error {
	stream, _ := c.Client.PullData(context.Background(), reqData)

	// 循环获取，仅当服务器结束，回调返回了false，才会结束
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
	log.Infof("连接服务 ip:%v, port:%v", Ip, Port)

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

// 几个辅助函数

// GobEncode 编码
func GobEncode(da interface{}) []byte {
	var buffer bytes.Buffer //缓冲区
	_ = gob.NewEncoder(&buffer).Encode(da)
	return buffer.Bytes()
}

// GobDecode 解码
func GobDecode(bv []byte, ret interface{}) error {
	var b bytes.Buffer //缓冲区
	b.Write(bv)
	if err := gob.NewDecoder(&b).Decode(ret); err != nil {
		return err
	}
	return nil
}

// ZlibEncode zlib压缩
func ZlibEncode(data []byte) []byte {
	var bufZipped bytes.Buffer
	zw := zlib.NewWriter(&bufZipped)
	defer zw.Close()

	if _, err := zw.Write(data); err != nil {
		log.Errorf("Failed to write data to zlib writer: %v", err)
		return nil
	}

	if err := zw.Close(); err != nil {
		log.Errorf("Failed to close zlib writer: %v", err)
		return nil
	}

	return bufZipped.Bytes()
}

// ZlibDecode zlib解压
func ZlibDecode(compressedData []byte) ([]byte, error) {
	// 创建zlib读取器
	zlibReader, err := zlib.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		log.Errorf("创建zlib读取器失败: %v", err)
		return nil, err
	}
	// 确保读取器关闭
	defer zlibReader.Close()

	// 读取解压后的数据
	decompressedData, err := io.ReadAll(zlibReader)
	if err != nil {
		log.Errorf("解压数据失败: %v", err)
		return nil, err
	}

	return decompressedData, nil
}
