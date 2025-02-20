package client

import (
	"bytes"
	"compress/zlib"
	"context"
	"fmt"
	protoMq "github.com/loudbund/go-mq/proto"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"io"
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
func (c *client) Pull(reqData *protoMq.PullDataReq, callBack func(*DataEvent) bool) error {
	stream, _ := c.Client.PullData(context.Background(), reqData)

	for {

		if res, err := stream.Recv(); err != nil {

			if err.Error() == "EOF" {
				return nil
			}
			log.Error(err)
			return err

		} else {

			// 解析数据
			topicMap, dataGroup := res.TopicMap, DataGroupDecode(res.DataMix)

			// 遍历收到的数据
			for _, v := range dataGroup.Items {

				// 频道名称
				topicName := ""
				if v.TopicId > 0 {
					topicName = topicMap[v.TopicId]
				}

				D := &DataEvent{
					Position: int64(dataGroup.Bucket)<<32 | int64(v.DataId),
					Topic:    topicName,
					Data:     v.Data,
				}

				if !callBack(D) {
					return nil
				}

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
	reader, err := zlib.NewReader(bytes.NewReader(compressedData))
	if err != nil {
		log.Errorf("创建zlib读取器失败: %v", err)
		return nil, err
	}
	// 确保读取器关闭
	defer reader.Close()

	// 读取解压后的数据
	decompressedData, err := io.ReadAll(reader)
	if err != nil {
		log.Errorf("解压数据失败: %v", err)
		return nil, err
	}

	return decompressedData, nil
}

// DataGroupEncode DataGroup结构数据序列化
func DataGroupEncode(Zlib bool, dataGroup *protoMq.DataGroup) []byte {
	var buffer bytes.Buffer

	// 循环序列化
	bytesContent, _ := proto.Marshal(dataGroup)

	// 压缩
	if Zlib {
		bytesContent = ZlibEncode(bytesContent)
		buffer.WriteByte(1)
	} else {
		buffer.WriteByte(0)
	}
	buffer.Write(bytesContent)

	return buffer.Bytes()
}

// DataGroupDecode DataGroup结构数据序列化
func DataGroupDecode(Data []byte) *protoMq.DataGroup {
	bytesContent := Data[1:]

	// 需要解压
	if Data[0] == 1 {
		B, err := ZlibDecode(bytesContent)
		if err != nil {
			log.Panic(err)
		}
		bytesContent = B
	}

	// 反序列化
	dataGroup := &protoMq.DataGroup{}
	if err := proto.Unmarshal(bytesContent, dataGroup); err != nil {
		log.Panic(err)
	}

	return dataGroup
}
