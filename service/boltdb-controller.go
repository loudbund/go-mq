package service

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/loudbund/go-mq/client"
	protoMq "github.com/loudbund/go-mq/proto"
	"github.com/loudbund/go-utils/utils_v1"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"os"
	"sync"
	"time"
)

// Writing 当前写入数据位置记录结构
type Writing struct {
	Bucket int32 // 当前bucket
	DataId int32 // 当前最新写入的数据id
}

// BucketHeader 数据bucket的info键值数据
type BucketHeader struct {
	MaxDataId int32 // 已封存的数据bucket的最大数据id
}

// TopicMap 频道id信息
type TopicMap struct {
	T2Id map[string]int32 // 频道名到频道id的映射
	Id2T map[int32]string // 频道id到频道名的映射
}

// Controller 数据控制入口
type Controller struct {
	dbHandle *bolt.DB // 数据库句柄
	writing  Writing  // 写入位置记录
	topicMap TopicMap // 频道id信息
}

var boltDbLock sync.RWMutex

// Init 初始化
func (c *Controller) Init() {
	// 加锁，防止并发访问数据库
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	// 重新打开数据库连接; 初始化频道ID映射; 初始化写入位置记录
	c.reOpenDb()
	c.initTopicMap()
	c.initWriting()

	// 调试：打印写入位置记录和频道ID映射
	fmt.Println(c.writing, c.topicMap)

	// 定时重新打开数据库
	go utils_v1.Time().SimpleMsgCron(make(chan bool), 1000*5, func(IsInterval bool) bool {
		boltDbLock.Lock()
		defer boltDbLock.Unlock()

		c.reOpenDb()
		return true
	})

	// 定时清理过期bucket
	go c.runExpireBucket()
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
		time.Sleep(time.Millisecond * 20)
	}

	// 打开db
	dbHandle, err := bolt.Open(CfgBoltDb.DataDirName+"/data.db", os.FileMode(os.O_RDWR), nil)
	if err != nil {
		log.Panic(err)
	}
	c.dbHandle = dbHandle
}

// WriteData 写入一条数据
func (c *Controller) WriteData(topicName string, data []byte) error {
	// 加锁，防止并发写入
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	// 本条数据信息: 频道ID;bucketId，以小时为单位;keyId，当前写入位置记录中的键ID加1
	topicId := c.getTopicIdFromName(topicName)
	bucket := int32((time.Now().Unix() / 3600) * 3600)
	dataId := c.writing.DataId + 1

	// bucket已切换:  需要刷新当前的数据bucket信息;新的数据bucket信息;keyId置为1
	if bucket != c.writing.Bucket {
		c.flushDataBucketHeader(c.writing.Bucket, c.writing.DataId)
		c.flushDataBucketHeader(bucket, 0)
		dataId = 1
	}

	// 写入数据; 刷新写入位置记录; 更新写入位置缓存变量
	c.flushDataBucketDetail(bucket, topicId, dataId, data)
	c.flushWriting(bucket, dataId)
	c.writing.Bucket, c.writing.DataId = bucket, dataId

	return nil
}

// GetData 获取一组数据
func (c *Controller) GetData(reqTopicMaps map[string]bool, cursorBucket, cursorDataId int32, maxLine int) (map[int32]string, *protoMq.DataGroup) {
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	DataItems := make([]*protoMq.DataItem, 0)
	topicId2Name := make(map[int32]string)

	// 1、指针位置位置太新:  比写入中的最新位置还新，返回写入中的最新位置
	if cursorBucket > c.writing.Bucket || cursorBucket == c.writing.Bucket && cursorDataId >= c.writing.DataId {
		return make(map[int32]string), &protoMq.DataGroup{
			Bucket: c.writing.Bucket,
			Items:  []*protoMq.DataItem{{DataId: c.writing.DataId}},
		}
	}

	// 2、指针bucket为写入中的bucket:  则从写入中的bucket里取
	if cursorBucket == c.writing.Bucket {
		analyzedDataId := c.getDbDataFromBucket(reqTopicMaps, cursorBucket, cursorDataId, c.writing.DataId, maxLine,
			&topicId2Name, &DataItems)

		// 补充一条含位置信息的空数据
		if len(DataItems) == 0 {
			DataItems = append(DataItems, &protoMq.DataItem{DataId: analyzedDataId, TopicId: 0})
		}

		return topicId2Name, &protoMq.DataGroup{
			Bucket: cursorBucket,
			Items:  DataItems,
		}
	}

	// 3、从未加压的bucket里取
	for {
		// 游标bucket大于等于写入中的bucket
		if cursorBucket >= c.writing.Bucket {
			break
		}

		// 游标的bucket信息读取和判断
		header := c.getDbHeaderFromBucket(cursorBucket)
		if header == nil || header.MaxDataId == 0 {
			if len(DataItems) > 0 {
				break
			}
			cursorBucket, cursorDataId = cursorBucket+3600, 0
			continue
		}

		// 读取一批数据
		cursorDataId = c.getDbDataFromBucket(reqTopicMaps, cursorBucket, cursorDataId, header.MaxDataId, maxLine,
			&topicId2Name, &DataItems)

		// 读取结束了
		if len(DataItems) > maxLine {
			break
		}

		//	游标的bucket数据已经读取完了
		if cursorDataId == header.MaxDataId {
			if len(DataItems) > 0 {
				break
			}
			cursorBucket, cursorDataId = cursorBucket+3600, 0
		}
	}

	if len(DataItems) == 0 {
		DataItems = append(DataItems, &protoMq.DataItem{DataId: cursorDataId, TopicId: 0})
	}

	return topicId2Name, &protoMq.DataGroup{
		Bucket: cursorBucket,
		Items:  DataItems,
	}
}

// View 查看数据库数据
func (c *Controller) View() {
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	c.reOpenDb()
	defer c.Destroy()

	// 遍历所有bucket
	c.dbHandle.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if string(name) == "Run" {
				c.initTopicMap()
				fmt.Println("频道集合:", c.topicMap)
				// 从数据库里读取byte数据
				Bytes, err := c.readKeyData(name, []byte("Writing"))
				if err != nil {
					log.Fatal(err)
				}
				// 如果读取到的数据长度不为0，将数据解析到缓存变量
				if err := json.Unmarshal(Bytes, &c.writing); err != nil {
					log.Panic(err)
				}
				dateTime := time.Unix(int64(c.writing.Bucket), 0)
				fmt.Println("BUCKET:", dateTime.Format("2006-01-02 15:04:05"), "DataId:", c.writing.DataId)
			} else {
				dateTime := time.Unix(int64(Byte2int32(name)), 0)
				header := c.getDbHeaderFromBucket(Byte2int32(name))
				if header == nil {
					fmt.Println("bucket:", dateTime.Format("2006-01-02 15:04:05"), "maxDataId:", -1)
				} else {
					fmt.Println("bucket:", dateTime.Format("2006-01-02 15:04:05"), "maxDataId:", header.MaxDataId)
				}
			}
			return nil
		})
	})
}

// Zip 查看数据库数据
func (c *Controller) Zip() {
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	c.reOpenDb()
	defer c.Destroy()

	// 遍历所有bucket
	c.dbHandle.Update(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if err := tx.DeleteBucket(name); err != nil {
				log.Panic(err)
			}
			fmt.Println(name)
			return nil
		})
	})

}

// 从db库里获取writing数据
func (c *Controller) getDbWriting() *Writing {
	// 从数据库里读取byte数据
	Bytes, err := c.readKeyData([]byte("Run"), []byte("Writing"))
	if err != nil {
		log.Fatal(err)
	}

	// 库里没有就需要初始化
	var writing *Writing
	if len(Bytes) == 0 {
		return nil
	}
	// 如果读取到的数据长度不为0，将数据解析到缓存变量
	if err := json.Unmarshal(Bytes, &writing); err != nil {
		log.Panic(err)
	}

	return writing
}

// 从db库里获取bucket的信息
func (c *Controller) getDbHeaderFromBucket(bucket int32) *BucketHeader {
	// 读取bucket的info数据
	byteInfo, err := c.readKeyData(Int322byte(bucket), []byte("Header"))
	if err != nil {
		log.Panic(err)
	}
	if len(byteInfo) == 0 {
		return nil
	}

	// 反序列化
	var header *BucketHeader
	err = gob.NewDecoder(bytes.NewBuffer(byteInfo)).Decode(&header)
	if err != nil {
		log.Panic(err)
	}

	return header
}

// 从db库里取数据
func (c *Controller) getDbDataFromBucket(
	reqTopicMaps map[string]bool, reqBucket, reqDataIdStart, reqDataIdEnd int32, maxLine int,
	topicId2Name *map[int32]string, DataItems *[]*protoMq.DataItem,
) int32 {
	analyzedDataId := reqDataIdStart

	for id := reqDataIdStart + 1; id <= reqDataIdEnd; id++ {
		// 获取详细数据信息
		detailInfo, err := c.readKeyData(Int322byte(reqBucket), Int322byte(id))
		if err != nil {
			log.Panic(err)
		}

		// 判断topic是否符合要求
		topicId := Byte2int32(detailInfo[0:4])
		topic := c.getTopicFromId(topicId)

		if _, ok := reqTopicMaps[topic]; ok {
			// 获取详细信息内容
			detail, err := c.readKeyData(Int322byte(reqBucket), Int322byte(id)[1:])
			if err != nil {
				log.Panic(err)
			}

			dataItem := protoMq.DataItem{}
			if err := proto.Unmarshal(detail, &dataItem); err != nil {
				log.Panic(err)
			}
			*DataItems = append(*DataItems, &dataItem)

			(*topicId2Name)[topicId] = topic
		}
		analyzedDataId = Byte2int32(detailInfo[4:8])

		// 数据量获取够了，跳出循环
		if len(*DataItems) >= maxLine {
			break
		}

	}

	return analyzedDataId
}

// 通过频道名获取频道id，已存在则返回id；
// 没有则创建,并将数据持久化写入数据库
func (c *Controller) getTopicIdFromName(topicName string) int32 {
	// 检查频道名是否已经存在于频道id映射中
	if topicId, ok := c.topicMap.T2Id[topicName]; ok {
		return topicId
	} else {
		// 取c.channelsIds.Channel2Id的最大值,并加1
		newTopicId := lo.Max(lo.Values(c.topicMap.T2Id)) + 1

		// 将新的频道id和频道名添加到频道id映射中
		c.topicMap.T2Id[topicName], c.topicMap.Id2T[newTopicId] = newTopicId, topicName
		c.flushTopicsIds()

		return newTopicId
	}
}

// 通过频道id获取频道名，没有则报错
func (c *Controller) getTopicFromId(topicId int32) string {
	// 检查频道id是否存在于频道id映射中
	if topicName, ok := c.topicMap.Id2T[topicId]; ok {
		// 如果存在，返回对应的频道名
		return topicName
	} else {
		// 如果不存在，记录错误日志并返回空字符串
		log.Panic("topicId not found")
		return ""
	}
}

// 从数据库里初始化位置记录
func (c *Controller) initWriting() {
	writing := c.getDbWriting()
	if writing == nil {
		// 变量初始化
		c.writing = Writing{Bucket: int32((time.Now().Unix() / 3600) * 3600), DataId: 0}
		// 持久化写入位置缓存变量到数据库，同时持久化bucket的info信息数据
		c.flushWriting(c.writing.Bucket, c.writing.DataId)
		c.flushDataBucketHeader(c.writing.Bucket, 0)
	} else {
		c.writing = *writing
	}
}

// 方法的主要功能是从数据库中读取频道ID信息，
// 并将其解析为 ChannelsIds 结构体的实例。
// 如果数据库中没有存储频道ID信息，则初始化一个新的 ChannelsIds 结构体实例。
func (c *Controller) initTopicMap() {
	// 初始化 ChannelsIds 结构体实例
	c.topicMap = TopicMap{T2Id: make(map[string]int32), Id2T: make(map[int32]string)}

	// 从数据库中读取频道id信息
	Bytes, err := c.readKeyData([]byte("Run"), []byte("TopicMap"))
	if err != nil {
		// 如果读取失败，记录错误日志并终止程序
		log.Fatal(err)
	}

	// 如果读取到的数据长度为0，直接返回
	if len(Bytes) == 0 {
		return
	}

	// 将数据解析到缓存变量
	if err := json.Unmarshal(Bytes, &c.topicMap.T2Id); err != nil {
		log.Panic(err)
	}
	// 遍历频道id信息，将频道id和频道名的映射关系反转
	for k, v := range c.topicMap.T2Id {
		c.topicMap.Id2T[v] = k
	}
}

// 刷新写入位置记录，将数据持久化写入数据库
func (c *Controller) flushWriting(bucket, dataId int32) {
	// 将写入位置记录转换为JSON格式
	writingInfo := Writing{
		Bucket: bucket,
		DataId: dataId,
	}
	D, err := json.Marshal(writingInfo)
	if err != nil {
		log.Panicf("Failed to marshal WritingInfo: %v", err)
	}

	// 将JSON数据写入数据库
	if err := c.writeKeyData([]byte("Run"), []byte("Writing"), D); err != nil {
		log.Panicf("Failed to write WritingInfo to database: %v", err)
	}
}

// 刷新频道id信息，将数据持久化写入数据库
func (c *Controller) flushTopicsIds() {
	// 将频道id信息转换为JSON格式
	D, err := json.Marshal(c.topicMap.T2Id)
	if err != nil {
		log.Panicf("Failed to marshal Channel2Id: %v", err)
		return
	}

	// 将JSON数据写入数据库
	if err := c.writeKeyData([]byte("Run"), []byte("TopicMap"), D); err != nil {
		log.Panicf("Failed to write ChannelId to database: %v", err)
	}
}

// 刷新bucket的info信息，将数据持久化写入数据库
func (c *Controller) flushDataBucketHeader(bucket, MaxDataId int32) {
	header := BucketHeader{MaxDataId: MaxDataId}

	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(header)
	if err != nil {
		log.Panicf("Failed to marshal BucketInfo: %v", err)
	}

	if err := c.writeKeyData(Int322byte(bucket), []byte("Header"), buf.Bytes()); err != nil {
		// 如果写入失败，记录错误日志并终止程序
		log.Panicf("Failed to write BucketInfo to database: %v", err)
	}
}

// 详细数据入库
func (c *Controller) flushDataBucketDetail(bucket int32, topicId int32, dataId int32, data []byte) {
	// 将 int32 类型的整数转换为字节切片
	bytesBucket := Int322byte(bucket)
	bytesDataId := Int322byte(dataId)
	bytesTopicId := Int322byte(topicId)

	// 创建一个字节缓冲区; 将 channelId 的字节切片写入缓冲区; 将数据内容写入缓冲区
	dataItem := protoMq.DataItem{
		DataId:  dataId,
		TopicId: topicId,
		Data:    data,
	}

	// 写入数据内容,key为后3个字节
	B, _ := proto.Marshal(&dataItem)
	if err := c.writeKeyData(bytesBucket, bytesDataId[1:], B); err != nil {
		log.Panic(err)
	}

	// 写入数据信息,key为4个字节
	var bufInfo bytes.Buffer
	bufInfo.Write(bytesTopicId)
	bufInfo.Write(bytesDataId)
	if err := c.writeKeyData(bytesBucket, bytesDataId, bufInfo.Bytes()); err != nil {
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

// 启动bucket清理服务
func (c *Controller) runExpireBucket() {
	// 先清理一遍
	c.clearExpireBucket()

	// 定时清理
	for {
		select {
		case <-time.NewTimer(time.Hour * 1).C:
			c.clearExpireBucket()
		}
	}
}

// ClearExpireBucket 清理过期的bucket数据
func (c *Controller) clearExpireBucket() {
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	// 超过保留时间的bucket将被删除
	expireTime := int(time.Now().Unix()) - CfgBoltDb.HourDataRetain*3600
	if err := c.dbHandle.Update(func(tx *bolt.Tx) error {
		if err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			if string(name) != "Run" {
				if Byte2int32(name) < int32(expireTime) {
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

// ZlibEncode zlib压缩
func ZlibEncode(data []byte) []byte {
	return client.ZlibEncode(data)
}

// ZlibDecode zlib解压
func ZlibDecode(compressedData []byte) ([]byte, error) {
	return client.ZlibDecode(compressedData)
}
