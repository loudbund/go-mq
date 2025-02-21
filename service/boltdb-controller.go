package service

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/loudbund/go-utils/utils_v1"
	"github.com/samber/lo"
	log "github.com/sirupsen/logrus"
	"os"
	"sync"
	"time"
)

type TTopicName string // 频道名称类型
type TTopicId int32    // 频道id类型
type TBucketId int32   // bucketId类型
type TDataId int32     // 数据id类型

// Running 当前写入数据位置记录结构
type Running struct {
	Bucket      TBucketId             // 当前bucket
	DataId      TDataId               // 当前最新写入的数据id
	BucketCache map[TBucketId]TDataId // bucket缓存，记录写入完成的bucket的最大DataId
}

// TopicMap 频道id信息
type TopicMap struct {
	T2Id map[TTopicName]TTopicId // 频道名到频道id的映射
	Id2T map[TTopicId]TTopicName // 频道id到频道名的映射
}

// Controller 数据控制入口
type Controller struct {
	dbHandle *bolt.DB // 数据库句柄
	running  Running  // 缓存中数据
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
	c.initRunning()

	// 调试：打印写入位置记录和频道ID映射
	log.Infof("running变量:%v", c.running)
	log.Infof("topicMap变量:%v", c.topicMap)

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
func (c *Controller) WriteData(topicName TTopicName, data []byte) error {
	// 加锁，防止并发写入
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	// bucketId，以小时为单位;keyId，
	bucket := TBucketId((time.Now().Unix() / 3600) * 3600)

	// bucket已切换:  需要刷新当前的数据bucket信息;新的数据bucket信息;keyId置为1
	if bucket != c.running.Bucket {
		c.flushDataBucketMaxDataId(c.running.Bucket, c.running.DataId)
		c.running.DataId = 0
	}

	// 计算新数据的频道id和数据id
	topicId := c.getTopicIdFromName(topicName)
	dataId := c.running.DataId + 1

	// 写入数据
	c.flushDataBucketDetail(bucket, topicId, dataId, data)

	// 数据写成功后，刷新变量里的bucket和数据id
	c.running.Bucket, c.running.DataId = bucket, dataId

	return nil
}

// GetData 获取一组数据
func (c *Controller) GetData(reqTopicMaps map[TTopicName]bool, curBucket TBucketId, curDataId TDataId) (
	TTopicName, TBucketId, TDataId, []byte,
) {
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	// 参数调整
	curBucket, curDataId = c.adjustGetDataParam(curBucket, curDataId)

	//
	for {
		// 1、异常了，还没有新数据
		if curBucket > c.running.Bucket {
			return "", c.running.Bucket, c.running.DataId, nil
		}

		// 2、指针bucket为写入中的bucket:  则从正在写入中的bucket里取
		if curBucket == c.running.Bucket {
			// 异常了，还没有新数据
			if curDataId >= c.running.DataId {
				return "", c.running.Bucket, c.running.DataId, nil
			}

			// 取出一条数据
			topicName, data := c.getDbDataFromBucket(reqTopicMaps, curBucket, curDataId+1)

			// 频道不符合要求
			if topicName == "" {
				curDataId = curDataId + 1
				continue
			}

			return topicName, curBucket, curDataId + 1, data
		}

		// 取历史bucket的最大数据id
		MaxDataId := c.getBucketMaxDataId(curBucket)

		// 历史bucket里可以取数据，则取出返回
		if curDataId < MaxDataId {
			// 取出一条数据
			topicName, data := c.getDbDataFromBucket(reqTopicMaps, curBucket, curDataId+1)

			// 频道不符合要求
			if topicName == "" {
				curDataId = curDataId + 1
				continue
			}

			return topicName, curBucket, curDataId + 1, data

		} else {

			// 则切到下一个bucket
			curBucket, curDataId = curBucket+3600, 0

		}
	}
}

// 位置参数矫正
func (c *Controller) adjustGetDataParam(curBucket TBucketId, curDataId TDataId) (TBucketId, TDataId) {
	// bucket名称为近30天内的，则位置不作矫正
	if curBucket > c.running.Bucket-3600*24*30 {
		return curBucket, curDataId
	}

	// bucket为0，则从30天前开始取数据
	if curBucket == 0 {
		return c.running.Bucket - 3600*24*30, 0
	}

	// bucket小于0，则只接受新数据
	if curBucket < 0 {
		return c.running.Bucket, c.running.DataId
	}

	return curBucket, curDataId
}

// View 查看数据库数据
func (c *Controller) View() {
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	defer c.Destroy()
	c.reOpenDb()
	c.initTopicMap()
	c.initRunning()

	dateTime := time.Unix(int64(c.running.Bucket), 0)
	fmt.Println("频道集合:", c.topicMap)
	fmt.Println("BUCKET:", dateTime.Format("2006-01-02 15:04:05"), "DataId:", c.running.DataId)

	// 遍历所有bucket
	c.dbHandle.View(func(tx *bolt.Tx) error {
		return tx.ForEach(func(name []byte, b *bolt.Bucket) error {

			if string(name) == "Set" {
			} else {

				dateTime := time.Unix(int64(Byte2int32(name)), 0)
				MaxDataId := c.getBucketMaxDataId(TBucketId(Byte2int32(name)))

				fmt.Println("bucket:", dateTime.Format("2006-01-02 15:04:05"), "maxDataId:", MaxDataId)
			}
			return nil
		})
	})
}

// 从db库里获取bucket的信息
func (c *Controller) getBucketMaxDataId(bucket TBucketId) TDataId {

	// 已经有缓存
	if MaxDataId, ok := c.running.BucketCache[bucket]; ok {
		return MaxDataId
	}

	// 读取bucket的MaxDataId数据
	byteMaxDataId, err := c.readKeyData(Int322byte(int32(bucket)), []byte("MaxDataId"))
	if err != nil {
		log.Panic(err)
	}

	if len(byteMaxDataId) == 0 {
		return 0
	}

	// 写入缓存
	MaxDataId := TDataId(Byte2int32(byteMaxDataId))
	c.running.BucketCache[bucket] = MaxDataId

	return MaxDataId
}

// 从db库查找最大的bucketId
func (c *Controller) getDbMaxBucket() TBucketId {
	MaxBucket := TBucketId(0)

	err := c.dbHandle.View(func(tx *bolt.Tx) error {

		err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {

			if string(name) != "Set" {

				if TBucketId(Byte2int32(name)) > MaxBucket {
					MaxBucket = TBucketId(Byte2int32(name))
				}
			}
			return nil
		})
		if err != nil {
			log.Panic(err)
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	return MaxBucket
}

// 从db库里查找最大的dataId
func (c *Controller) getDbMaxDataIdFromBucket(bucket TBucketId) TDataId {
	MaxDataId := TDataId(0)

	err := c.dbHandle.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(Int322byte(int32(bucket)))

		if b == nil {
			return nil
		}

		// 查找最大的dataId，直接定位到最后一个key
		cur := b.Cursor()
		k, _ := cur.Last()
		if len(k) != 4 {
			log.Panicf("查找最大的dataid失败")
		}

		MaxDataId = TDataId(Byte2int32(k))

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	return MaxDataId
}

// 从db库里取数据
func (c *Controller) getDbDataFromBucket(topicMaps map[TTopicName]bool, bucket TBucketId, dataId TDataId) (TTopicName, []byte) {
	// 从boltDb里读取
	detail, err := c.readKeyData(Int322byte(int32(bucket)), Int322byte(int32(dataId)))
	if err != nil {
		log.Panic(err)
	}

	// 频道id匹配，返回内容数据，否则返回nil
	topicId := TTopicId(Byte2int32(detail[0:4]))
	topicName := c.getTopicFromId(topicId)

	if _, ok := topicMaps[topicName]; ok {
		return topicName, detail[4:]
	} else {
		return "", nil
	}
}

// 通过频道名获取频道id，已存在则返回id；
// 没有则创建,并将数据持久化写入数据库
func (c *Controller) getTopicIdFromName(topicName TTopicName) TTopicId {

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
func (c *Controller) getTopicFromId(topicId TTopicId) TTopicName {

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
func (c *Controller) initRunning() {
	// 找最大的bucketId
	MaxBucket := c.getDbMaxBucket()

	// 没有找到最大的bucketId，则初始化一个新的位置记录
	if MaxBucket == 0 {
		c.running = Running{
			Bucket:      TBucketId((time.Now().Unix() / 3600) * 3600),
			DataId:      0,
			BucketCache: make(map[TBucketId]TDataId),
		}
		return
	}

	//	找最大的dataId
	MaxDataId := c.getDbMaxDataIdFromBucket(MaxBucket)
	c.running = Running{
		Bucket:      MaxBucket,
		DataId:      MaxDataId,
		BucketCache: make(map[TBucketId]TDataId),
	}
}

// 方法的主要功能是从数据库中读取频道ID信息，
// 并将其解析为 ChannelsIds 结构体的实例。
// 如果数据库中没有存储频道ID信息，则初始化一个新的 ChannelsIds 结构体实例。
func (c *Controller) initTopicMap() {
	// 初始化 ChannelsIds 结构体实例
	c.topicMap = TopicMap{T2Id: make(map[TTopicName]TTopicId), Id2T: make(map[TTopicId]TTopicName)}

	// 从数据库中读取频道id信息
	Bytes, err := c.readKeyData([]byte("Set"), []byte("TopicMap"))
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

// 刷新频道id信息，将数据持久化写入数据库
func (c *Controller) flushTopicsIds() {
	// 将频道id信息转换为JSON格式
	D, err := json.Marshal(c.topicMap.T2Id)
	if err != nil {
		log.Panicf("Failed to marshal Channel2Id: %v", err)
		return
	}

	// 将JSON数据写入数据库
	if err := c.writeKeyData([]byte("Set"), []byte("TopicMap"), D); err != nil {
		log.Panicf("Failed to write ChannelId to database: %v", err)
	}
}

// 刷新bucket的info信息，将数据持久化写入数据库
func (c *Controller) flushDataBucketMaxDataId(bucket TBucketId, MaxDataId TDataId) {
	if err := c.writeKeyData(Int322byte(int32(bucket)), []byte("MaxDataId"), Int322byte(int32(MaxDataId))); err != nil {

		// 如果写入失败，记录错误日志并终止程序
		log.Panicf("Failed to write BucketInfo to database: %v", err)
	}
}

// 详细数据入库
func (c *Controller) flushDataBucketDetail(bucket TBucketId, topicId TTopicId, dataId TDataId, data []byte) {

	var bufInfo bytes.Buffer
	bufInfo.Write(Int322byte(int32(topicId)))
	bufInfo.Write(data)

	if err := c.writeKeyData(Int322byte(int32(bucket)), Int322byte(int32(dataId)), bufInfo.Bytes()); err != nil {
		log.Panic(err)
	}
}

// 从数据库里读取一个bucket里的一个键值的数据
func (c *Controller) readKeyData(bucketName, key []byte) ([]byte, error) {

	// 读取数据
	var dRet = make([]byte, 0)

	err := c.dbHandle.View(func(tx *bolt.Tx) error {

		if b := tx.Bucket(bucketName); b != nil {
			dRet = b.Get(key)
		}

		return nil

	})

	if err != nil {

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

	err := c.dbHandle.Update(func(tx *bolt.Tx) error {

		err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {

			if string(name) != "Set" {

				if Byte2int32(name) < int32(expireTime) {

					if err := tx.DeleteBucket(name); err != nil {
						log.Panic(err)
					}
				}
			}
			return nil
		})

		if err != nil {
			log.Panic(err)
		}

		return nil
	})

	if err != nil {
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
