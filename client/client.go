package client

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	mqV1 "github.com/loudbund/go-mq/api/v1"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"io"
	"time"
)

// VersionClient 版本号
var VersionClient = "1.0.0"

func init() {
}

// DataEvent 响应回调数据结构
type DataEvent struct {
	Position int64  // 数据位置
	Topic    string // 数据频道
	Data     []byte // 数据内容
}

// Client 客户端实例
type Client struct {
	mqClient mqV1.MqClient
}

// Pull 数据拉取
func (c *Client) Pull(reqData *mqV1.PullDataReq, callBack func(res *mqV1.PullDataRes) bool) error {
	stream, err := c.mqClient.PullData(context.Background(), reqData)

	defer func() { _ = stream.CloseSend() }()

	if err != nil {
		log.Error(err)
		return err
	}

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

// Push 推送数据
func (c *Client) Push(dataPush *mqV1.PushDataReq) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	if r, err := c.mqClient.PushData(ctx, dataPush); err != nil {
		log.Errorf("写入mq失败: %v", err)
		return err
	} else if r.ErrNum != 0 {
		log.Errorf("写入mq失败: ErrNum:%d msg:%s", r.ErrNum, r.Msg)
		return errors.New(r.Msg)
	}

	return nil
}

// NewClient 实例化客户端
func NewClient(Ip string, Port int) (*Client, error) {
	c := &Client{}
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
	c.mqClient = mqV1.NewMqClient(dial)

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

// Encode  结构体数据编码
func Encode(data interface{}, zLib bool) []byte {
	var bf bytes.Buffer
	bv := GobEncode(data)
	if zLib {
		bv = ZlibEncode(bv)
		bf.WriteByte(1)
	} else {
		bf.WriteByte(0)
	}
	bf.Write(bv)
	return bf.Bytes()
}

// Decode  结构体数据解码
func Decode(valueByte []byte, ret interface{}) error {
	var dataByte []byte
	if valueByte[0] == 1 {
		if b, err := ZlibDecode(valueByte[1:]); err != nil {
			return err
		} else {
			dataByte = b
		}
	} else {
		dataByte = valueByte[1:]
	}
	if err := GobDecode(dataByte, ret); err != nil {
		return nil
	}
	return nil
}

// PasswordEncode 密码加密
func PasswordEncode(password string) string {
	hash := md5.New()
	hash.Write([]byte(password))
	hashValue := hash.Sum(nil)
	return hex.EncodeToString(hashValue)
}
