package service

import (
	"fmt"
	"github.com/boltdb/bolt"
	log "github.com/sirupsen/logrus"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var boltDbLock sync.RWMutex

// Controller 数据控制入口
type Controller struct {
}

// RunClearExpireBucket 启动bucket清理服务
func RunClearExpireBucket() {
	// 先清理一遍
	(&Controller{}).ClearExpireBucket()

	// 定时清理
	for {
		select {
		case <-time.NewTimer(time.Hour * 1).C:
			(&Controller{}).ClearExpireBucket()
		}
	}
}

// PullSetChannelPosition
// 设置用户拉取频道数据位置
// 场景:
// 1、用户拉取数据时，指定要求刷新拉取位置时，将调用此函数刷新位置
// 2、另外系统发送完一批数据给用户后，将调用此函数刷新位置
func (c *Controller) PullSetChannelPosition(userName, channelName, bucketName, bucketKey string) error {
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	// 打开db
	dbHandle, err := bolt.Open(CfgBoltDb.DbFolder+"/"+CfgBoltDb.SysDbName, os.FileMode(os.O_RDWR), nil)
	if err != nil {
		log.Panic(err)
	}
	defer func() { _ = dbHandle.Close() }()

	// bucketName为空，使用当前的bucket name，和当前的bucket key；
	// bucketName有值，bucketKey为空，则使用第一个key
	okBucketName, okBucketKey := bucketName, bucketKey
	if bucketName == "" {
		if okBucketName, okBucketKey, err = (&SysDb{}).PullGetChannelPosition(dbHandle, channelName); err != nil {
			log.Error(err)
			return err
		}
		//fmt.Println(okBucketName, okBucketKey)
	} else if bucketKey == "" {
		bucketKey = strconv.FormatInt((&BNHelper{}).GetBucketStartKey(), 10)
	}

	// 刷新bolt数据库里用户拉取位置记录
	if err := (&SysDb{}).PullWriteUserPosition(dbHandle, userName, channelName, okBucketName, okBucketKey); err != nil {
		log.Error(err)
		return err
	}
	return nil
}

// PullGetData 读取一批数据
// 场景：消费用户连上服务器后，服务器循环调用次函数获取将要发送的数据
// 注: retBucketKey为"0"时表示切换到新的bucket，但bucket里还没有数据
func (c *Controller) PullGetData(userName, channelName string) (retData []string, retBucketName, retBucketKey string, retErr error) {
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	// 打开db1
	dbHandle, err := bolt.Open(CfgBoltDb.DbFolder+"/"+CfgBoltDb.PreChannelDb+channelName+".db", os.FileMode(os.O_RDWR), nil)
	if err != nil {
		log.Error(err)
		retErr = err
		return
	}
	defer func() { _ = dbHandle.Close() }()

	// 打开db2
	dbHandleSys, err1 := bolt.Open(CfgBoltDb.DbFolder+"/"+CfgBoltDb.SysDbName, os.FileMode(os.O_RDWR), nil)
	if err1 != nil {
		log.Error(err1)
		retErr = err1
		return
	}
	defer func() { _ = dbHandleSys.Close() }()

	// 读取数据
	retData, retErr = make([]string, 0), nil
	if bucketName, bucketKey, err := (&SysDb{}).PullGetUserPosition(dbHandleSys, userName, channelName); err != nil {
		retErr = err
		log.Error(err)
		return
	} else {
		// 读取
		if err := dbHandle.View(func(tx *bolt.Tx) error {
			// 状态桶里查询数据桶状态
			if bucket := tx.Bucket([]byte(bucketName)); bucket == nil {
				// bucket是历史的，切换到下一个
				if bucketName < (&BNHelper{}).GetBucketName() {
					retBucketName = (&BNHelper{}).GetBucketName()
					retBucketKey = "0"
				}
				return nil
			} else {
				cur := bucket.Cursor()
				readNum := 0
				k, v := cur.Seek([]byte(bucketKey))
				if bucketKey == "0" {
					k, v = cur.First()
				}
				for ; k != nil; k, v = cur.Next() {
					if bucketKey == string(k) {
						continue
					}
					retData = append(retData, string(v))
					bucketKey = string(k)

					readNum++
					if readNum == 100 {
						break
					}
				}
				retBucketName = bucketName
				retBucketKey = bucketKey
				// bucket是历史的，切换到下一个
				if readNum != 100 && bucketName < (&BNHelper{}).GetBucketName() {
					retBucketName = (&BNHelper{}).GetBucketName()
					retBucketKey = "0"
				}
			}
			return nil
		}); err != nil {
			retErr = err
			log.Error(err)
		}
	}
	return
}

// PushWriteData 写入一批数据
// 场景：服务收到用户写入数据时，直接调用这个方法将数据写入boltDb
func (c *Controller) PushWriteData(channelName string, Data [][]byte) error {
	(&ChannelDb{}).Init(channelName)

	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	err := (&ChannelDb{}).PushWrite(channelName, Data)
	if err != nil {
		log.Error(err)
	}
	return err
}

// ClearExpireBucket 清理过期的bucket数据
func (c *Controller) ClearExpireBucket() {
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	// 遍历数据库
	if err := filepath.Walk(CfgBoltDb.DbFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if strings.Contains(path, CfgBoltDb.PreChannelDb) {
				if err := clearOneDb(path); err != nil {
					log.Error(err)
				}
			}
		}
		return nil
	}); err != nil {
		log.Error(err)
	}
}

// 清理一个仓库
func clearOneDb(channelPath string) (retErr error) {
	// 打开db1
	dbHandle, err := bolt.Open(channelPath, os.FileMode(os.O_RDWR), nil)
	if err != nil {
		log.Error(err)
		retErr = err
		return
	}
	defer func() { _ = dbHandle.Close() }()

	if err := dbHandle.Update(func(tx *bolt.Tx) error {
		tDiff, _ := time.ParseDuration(fmt.Sprintf("%dh", -CfgBoltDb.HourDataRetain))
		minBucketName := (&BNHelper{}).GetBucketName(time.Now().Add(tDiff))
		if err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
			// 太老的bucket，或者不规范的bucket，需要删除
			if string(name)[0:1] == "D" && // 只清理D打头的
				(len(string(name)) != len(minBucketName) || // bucket名称长度不对
					string(name) < minBucketName) { // bucket超期了
				if err := tx.DeleteBucket(name); err != nil {
					log.Panic(err)
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
