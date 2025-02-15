package service

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
	"time"
)

// DataGetOne 读取的一条数据格式
type DataGetOne struct {
	Position int64 // 高32位是bucketid, 低32位是序号号id
	Channel  string
	Data     []byte
}

// WritingInfo info bucket的
type WritingInfo struct {
	BucketId int32 // 当前bucketid
	MaxSId   int32 // 当前已写入的序号号id
}

// BucketInfo 数据bucket的info键值数据
type BucketInfo struct {
	MaxSId int32 // 当前已写入的序号号id
}

// ChannelsIds 频道id信息
type ChannelsIds struct {
	Channel2Id map[string]int32 // 频道名到id的映射
	Bucket2Id  map[int32]string // bucketid到频道名的映射
}

// Controller 数据控制入口
type Controller struct {
	dbHandle    *bolt.DB    // 数据库句柄
	writingInfo WritingInfo // 写入位置记录
	channelsIds ChannelsIds // 频道id信息
}

var boltDbLock sync.RWMutex

// Init 初始化
func (c *Controller) Init() {
	// 加锁，防止并发访问数据库
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	// 重新打开数据库连接; 初始化频道ID映射; 初始化写入位置记录
	c.reOpenDb()
	c.initChannelsIds()
	c.initWriteInfo()

	// 调试：获取频道"user"的ID
	c.getChannelIdFromName("user")

	// 调试：打印写入位置记录和频道ID映射
	fmt.Println(c.writingInfo, c.channelsIds)
}

// Destroy 关闭数据库句柄
func (c *Controller) Destroy() {
	if c.dbHandle != nil {
		c.dbHandle.Close()
	}
}

// 重新打开db
func (c *Controller) reOpenDb() {
	// 关闭db
	if c.dbHandle != nil {
		_ = c.dbHandle.Close()
	}

	// 打开db
	dbHandle, err := bolt.Open("data/data.db", os.FileMode(os.O_RDWR), nil)
	if err != nil {
		log.Panic(err)
	}
	c.dbHandle = dbHandle
}

// WriteData 写入一条数据
func (c *Controller) WriteData(channelName string, data []byte) error {
	// 加锁，防止并发写入
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	// 本条数据信息: 频道ID;bucketId，以小时为单位;keyId，当前写入位置记录中的键ID加1
	channelId := c.getChannelIdFromName(channelName)
	bucketId := int32((time.Now().Unix() / 3600) * 3600)
	keyId := c.writingInfo.MaxSId + 1

	// 需要切换bucket: 需要刷新当前的数据bucket信息;新的数据bucket信息;keyId置为1
	if bucketId != c.writingInfo.BucketId {
		c.flushDataBucketInfo(c.writingInfo.BucketId, c.writingInfo.MaxSId)
		c.flushDataBucketInfo(bucketId, 0)
		keyId = 1
	}

	// 写入数据; 刷新写入位置记录; 更新写入位置缓存变量
	c.flushDataBucketDetail(bucketId, channelId, keyId, data)
	c.flushWritingInfo(bucketId, keyId)
	c.writingInfo.BucketId, c.writingInfo.MaxSId = bucketId, keyId

	return nil
}

// GetData 获取一组数据
func (c *Controller) GetData(channels map[string]bool, p int64, maxLine int) []DataGetOne {
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	// 限制获取条数
	if maxLine > CfgBoltDb.DataMaxRowCurGet {
		maxLine = CfgBoltDb.DataMaxRowCurGet
	}

	// 变量初始化
	Data := make([]DataGetOne, 0)
	curPos := p
	var curData DataGetOne

	// 循环获取数据
	for len(Data) < maxLine {
		curData = c.nextData(curPos)
		// 位置没有变化了，说明没有新数据了,可以终止获取了
		if curData.Position == curPos {
			break
		}
		// 取到的数据有效
		if curData.Channel != "" {
			if _, ok := channels[curData.Channel]; ok {
				Data = append(Data, curData)
			}
		}
		curPos = curData.Position
	}

	// 补充一条含位置信息的空数据
	if len(Data) == 0 {
		Data = append(Data, DataGetOne{Position: curData.Position, Channel: "", Data: nil})
	}
	return Data
}

// GetNextData 读取一条数据
func (c *Controller) nextData(p int64) DataGetOne {
	// 右移32位获取高32位; 与操作获取低32位
	cBucketId, cId := int32(p>>32), int32(p&0xFFFFFFFF)

	// 1、没有新数据了，直接返回标记
	if c.writingInfo.BucketId == cBucketId && c.writingInfo.MaxSId == cId {
		return DataGetOne{Position: p, Channel: "", Data: nil}
	}

	// 获取下一个position信息
	nBucketId, nId := c.getNextPosition(cBucketId, cId)

	// 2、没有新数据了，直接返回标记
	if nBucketId == cBucketId && nId == cId {
		return DataGetOne{Position: p, Channel: "", Data: nil}
	}

	// 3、刚切换了bucket，只返回新的位置数据
	if nId == 0 {
		return DataGetOne{Position: (int64(nBucketId) << 32) | int64(0), Channel: "", Data: nil}
	}

	// 获取详细数据
	Detail, err := c.readKeyData(Int322byte(nBucketId), Int322byte(nId))
	if err != nil {
		log.Panic(err)
	}
	chName := c.getChannelNameFromId(Byte2int32(Detail[0:4]))

	// 4、返回详细数据
	return DataGetOne{
		Position: (int64(nBucketId) << 32) | int64(nId),
		Channel:  chName,
		Data:     Detail[4:],
	}
}

// 获取下一个position信息,返回 bucketId, KeyId
func (c *Controller) getNextPosition(bId, kId int32) (int32, int32) {
	// 1、异常
	if bId > c.writingInfo.BucketId {
		log.Panic("分组id异常")
	}

	// 2、正在获取当前写入的bucket数据
	// bucket没有变化，且id大于等于缓存位置变量里的id，可认为数据没有变化
	if bId == c.writingInfo.BucketId && kId >= c.writingInfo.MaxSId {
		return bId, c.writingInfo.MaxSId
	}
	// bucket没有变化，且id小于缓存位置变量里的id，位置加1即可
	if bId == c.writingInfo.BucketId && kId < c.writingInfo.MaxSId {
		return bId, kId + 1
	}

	// 3、从历史的bucket中取数据
	// 先获取这个bucket的info数据
	bInfoByte, err := c.readKeyData(Int322byte(bId), []byte("BucketInfo"))
	if err != nil {
		log.Panic(err)
		return 0, 0
	}
	// 没有取到bucket的info数据，说明bucket不存在，直接返回下一个bucket的id
	if len(bInfoByte) == 0 {
		return bId + 3600, 0
	}
	// 解析bucket的info数据
	var Info BucketInfo
	if err := json.Unmarshal(bInfoByte, &Info); err != nil {
		log.Panic(err)
	}
	// 当前bucket数据还未取完，keyId加1即可
	if kId < Info.MaxSId {
		return bId, kId + 1
	}
	// 当前bucket数据已经取完了，切换到下一个bucket，且键id从0开始
	return bId + 3600, 0
}

// RunClearExpireBucket 启动bucket清理服务
func RunClearExpireBucket() {
	// 先清理一遍
	(&Controller{}).ClearExpireBucket()

	// 定时清理
	for {
		select {
		case <-time.NewTimer(time.Hour * 1).C:
			//(&Controller{}).ClearExpireBucket()
		}
	}
}

// ClearExpireBucket 清理过期的bucket数据
func (c *Controller) ClearExpireBucket() {
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	// 打开db
	dbHandle, err := bolt.Open("data/data.db", os.FileMode(os.O_RDWR), nil)
	if err != nil {
		log.Error(err)
		return
	}
	defer func() { _ = dbHandle.Close() }()

	// 清理过期的bucket
	// 超过保留时间的bucket将被删除
	if err := dbHandle.Update(func(tx *bolt.Tx) error {
		if err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if string(name) != "info" {
				if false {
					if err := tx.DeleteBucket(name); err != nil {
						log.Panic(err)
					}
				}
			}
			return nil
		}); err != nil {
			log.Panic(err)
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	return
}

// 通过频道名获取频道id，已存在则返回id；
// 没有则创建,并将数据持久化写入数据库
func (c *Controller) getChannelIdFromName(channelName string) int32 {
	// 检查频道名是否已经存在于频道id映射中
	if chName, ok := c.channelsIds.Channel2Id[channelName]; ok {
		return chName
	} else {
		// 取c.channelsIds.Channel2Id的最大值,并加1
		newChId := lo.Max(lo.Values(c.channelsIds.Channel2Id)) + 1

		// 将新的频道id和频道名添加到频道id映射中
		c.channelsIds.Channel2Id[channelName], c.channelsIds.Bucket2Id[newChId] = newChId, channelName
		c.flushChannelsIds()

		return newChId
	}
}

// 通过频道id获取频道名，没有则报错
func (c *Controller) getChannelNameFromId(channelId int32) string {
	// 检查频道id是否存在于频道id映射中
	if V, ok := c.channelsIds.Bucket2Id[channelId]; ok {
		// 如果存在，返回对应的频道名
		return V
	} else {
		// 如果不存在，记录错误日志并返回空字符串
		log.Panic("channelId not found")
		return ""
	}
}

// 从数据库里初始化位置记录
func (c *Controller) initWriteInfo() {
	// 从数据库里读取byte数据
	Bytes, err := c.readKeyData([]byte("Info"), []byte("WritingInfo"))
	if err != nil {
		log.Fatal(err)
	}
	// 库里没有就需要初始化
	if len(Bytes) == 0 {
		// 变量初始化
		c.writingInfo = WritingInfo{BucketId: int32((time.Now().Unix() / 3600) * 3600), MaxSId: 0}
		// 持久化写入位置缓存变量到数据库，同时持久化bucket的info信息数据
		c.flushWritingInfo(c.writingInfo.BucketId, c.writingInfo.MaxSId)
		c.flushDataBucketInfo(c.writingInfo.BucketId, -1)
		return
	}
	// 如果读取到的数据长度不为0，将数据解析到缓存变量
	if err := json.Unmarshal(Bytes, &c.writingInfo); err != nil {
		log.Panic(err)
	}
}

// 方法的主要功能是从数据库中读取频道ID信息，
// 并将其解析为 ChannelsIds 结构体的实例。
// 如果数据库中没有存储频道ID信息，则初始化一个新的 ChannelsIds 结构体实例。
func (c *Controller) initChannelsIds() {
	// 初始化 ChannelsIds 结构体实例
	c.channelsIds = ChannelsIds{Channel2Id: make(map[string]int32), Bucket2Id: make(map[int32]string)}

	// 从数据库中读取频道id信息
	Bytes, err := c.readKeyData([]byte("Info"), []byte("ChannelId"))
	if err != nil {
		// 如果读取失败，记录错误日志并终止程序
		log.Fatal(err)
	}

	// 如果读取到的数据长度为0，直接返回
	if len(Bytes) == 0 {
		return
	}

	// 将数据解析到缓存变量
	if err := json.Unmarshal(Bytes, &c.channelsIds.Channel2Id); err != nil {
		log.Panic(err)
	}
	// 遍历频道id信息，将频道id和频道名的映射关系反转
	for k, v := range c.channelsIds.Channel2Id {
		c.channelsIds.Bucket2Id[v] = k
	}
}

// 刷新写入位置记录，将数据持久化写入数据库
func (c *Controller) flushWritingInfo(bucketId, MaxSId int32) {
	// 将写入位置记录转换为JSON格式
	writingInfo := WritingInfo{
		BucketId: bucketId,
		MaxSId:   MaxSId,
	}
	D, err := json.Marshal(writingInfo)
	if err != nil {
		log.Panicf("Failed to marshal WritingInfo: %v", err)
	}

	// 将JSON数据写入数据库
	if err := c.writeKeyData([]byte("Info"), []byte("WritingInfo"), D); err != nil {
		log.Panicf("Failed to write WritingInfo to database: %v", err)
	}
}

// 刷新频道id信息，将数据持久化写入数据库
func (c *Controller) flushChannelsIds() {
	// 将频道id信息转换为JSON格式
	D, err := json.Marshal(c.channelsIds.Channel2Id)
	if err != nil {
		log.Panicf("Failed to marshal Channel2Id: %v", err)
		return
	}

	// 将JSON数据写入数据库
	if err := c.writeKeyData([]byte("Info"), []byte("ChannelId"), D); err != nil {
		log.Panicf("Failed to write ChannelId to database: %v", err)
	}
}

// 刷新bucket的info信息，将数据持久化写入数据库
func (c *Controller) flushDataBucketInfo(bucketId, MaxSId int32) {
	// 创建一个 BucketInfo 结构体实例，设置 MaxSId 字段
	info := BucketInfo{MaxSId: MaxSId}
	// 将 BucketInfo 结构体序列化为 JSON 格式
	D, err := json.Marshal(info)
	if err != nil {
		// 如果序列化失败，记录错误日志并终止程序
		log.Panicf("Failed to marshal BucketInfo: %v", err)
	}
	// 将 JSON 格式的数据写入数据库中
	if err := c.writeKeyData(Int322byte(bucketId), []byte("BucketInfo"), D); err != nil {
		// 如果写入失败，记录错误日志并终止程序
		log.Panicf("Failed to write BucketInfo to database: %v", err)
	}
}

// 详细数据入库
func (c *Controller) flushDataBucketDetail(bucketId, channelId, Id int32, data []byte) {
	// 将 int32 类型的整数转换为字节切片
	BytesBucketId := Int322byte(bucketId)
	BytesId := Int322byte(Id)
	BytesChannelId := Int322byte(channelId)

	// 创建一个字节缓冲区; 将 channelId 的字节切片写入缓冲区; 将数据内容写入缓冲区
	var buffer bytes.Buffer
	buffer.Write(BytesChannelId)
	buffer.Write(data)

	// 将缓冲区中的数据写入到数据库的指定桶中
	if err := c.writeKeyData(BytesBucketId, BytesId, buffer.Bytes()); err != nil {
		log.Panic(err)
	}
}

// 从数据库里读取一个bucket里的一个键值的数据
func (c *Controller) readKeyData(bucketName, key []byte) ([]byte, error) {
	// 读取数据
	var dRet = make([]byte, 0)
	if err := c.dbHandle.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket(bucketName); b != nil {
			dRet = b.Get(key)
		}
		return nil
	}); err != nil {
		log.Fatal(err)
		return nil, err
	}
	return dRet, nil
}

// 向数据库里写入一个bucket里的一个键值的数据
func (c *Controller) writeKeyData(bucketName, key, value []byte) error {
	return c.dbHandle.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists(bucketName)
		if err != nil {
			log.Panic(err)
			return err
		}
		return bucket.Put(key, value)
	})
}

// Int642byte 将 int64 类型的整数转换为字节切片
func Int642byte(num int64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(num))
	return buf
}

// Byte2int64 将字节切片转换为 int64 类型的整数
func Byte2int64(b []byte) int64 {
	if len(b) != 8 {
		log.Panic("invalid byte slice length for int64 conversion")
	}
	return int64(binary.BigEndian.Uint64(b))
}

// Int322byte 将 int32 类型的整数转换为字节切片
func Int322byte(num int32) []byte {
	buf := new(bytes.Buffer)
	if err := binary.Write(buf, binary.BigEndian, num); err != nil {
		log.Panic(err)
	}
	return buf.Bytes()
}

// Byte2int32 将字节切片转换为 int32 类型的整数
func Byte2int32(b []byte) int32 {
	if len(b) != 4 {
		log.Panic("invalid byte slice length for int32 conversion")
	}
	return int32(binary.BigEndian.Uint32(b))
}
