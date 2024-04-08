package service

import (
	"errors"
	"github.com/boltdb/bolt"
	"github.com/loudbund/go-utils/utils_v1"
	log "github.com/sirupsen/logrus"
	"os"
	"strconv"
)

// ChannelDb 库${channel}.db 操作封装
type ChannelDb struct {
}

// Init 初始化
// 场景: 写入频道数据前，先调用这个函数确保频道数据库已创建
func (c *ChannelDb) Init(channelName string) {
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	if !utils_v1.File().CheckFileExist(CfgBoltDb.DbFolder + "/" + channelName + ".db") {
		if h, err := bolt.Open(CfgBoltDb.DbFolder+"/"+CfgBoltDb.PreChannelDb+channelName+".db", os.FileMode(os.O_RDWR), nil); err != nil {
			log.Panic("Init channel db error: ", err)
		} else {
			func() { _ = h.Close() }()
		}
	}
}

// PushGetChannelPosition 计算将要写入channel的bucket和位置
// 获取频道最新数据的位置信息
func (c *ChannelDb) PushGetChannelPosition(dbHandle *bolt.DB) (retBucketName string, retNextBucketKey int64, retErr error) {
	// 读取
	if err := dbHandle.Update(func(tx *bolt.Tx) error {
		retBucketName = (&BNHelper{}).GetBucketName()
		// 状态桶里查询数据桶状态
		if bucket := tx.Bucket([]byte(retBucketName)); bucket == nil {
			if _, retErr = tx.CreateBucket([]byte(retBucketName)); retErr != nil {
				return nil
			}
			retNextBucketKey = (&BNHelper{}).GetBucketStartKey()
		} else {
			cur := bucket.Cursor()
			if k, _ := cur.Last(); len(k) == 0 {
				retNextBucketKey = (&BNHelper{}).GetBucketStartKey()
			} else {
				if key, err := strconv.ParseInt(string(k), 10, 64); err != nil {
					log.Error(err)
					retErr = err
				} else {
					retNextBucketKey = key + 1
				}
			}
		}
		return nil
	}); err != nil {
		log.Error(err)
		retErr = err
	}
	return
}

// PushWrite 写入频道数据
// 直接写入频道数据到boltdb
// 场景:收到用户传来的数据时直接调用这个数据
func (c *ChannelDb) PushWrite(channelName string, data [][]byte) (retErr error) {
	// 打开db
	dbHandle, err := bolt.Open(CfgBoltDb.DbFolder+"/"+CfgBoltDb.PreChannelDb+channelName+".db", os.FileMode(os.O_RDWR), nil)
	if err != nil {
		log.Error(err)
		retErr = err
		return
	}
	defer func() { _ = dbHandle.Close() }()

	// 获取bucketName和bucketKey位置
	bucketName, nextBucketKey := "", int64(0)
	if bucketName, nextBucketKey, err = c.PushGetChannelPosition(dbHandle); err != nil {
		log.Error(err)
		return err
	}

	// 写入数据
	if err := dbHandle.Update(func(tx *bolt.Tx) error {
		var bucket *bolt.Bucket
		if bucket = tx.Bucket([]byte(bucketName)); bucket == nil {
			retErr = errors.New("bucket不存在")
			log.Error(retErr)
			return nil
		}

		// 数据写入
		for _, v := range data {
			if err := bucket.Put([]byte(strconv.FormatInt(nextBucketKey, 10)), v); err != nil {
				log.Panic(err)
			}
			//fmt.Println(utils_v1.Time().DateTime(), bucketName, nextBucketKey, len(v))
			nextBucketKey++
		}

		// 刷新位置和激活状态
		if err := (&SysDb{}).PushRefreshChannelPosition(channelName, bucketName, strconv.FormatInt(nextBucketKey-1, 10)); err != nil {
			retErr = err
			log.Error(err)
			return nil
		}

		return nil
	}); err != nil {
		log.Fatal(err)
	}

	return retErr
}
