package service

import (
	"errors"
	"github.com/boltdb/bolt"
	"github.com/loudbund/go-json/json_v1"
	"github.com/loudbund/go-utils/utils_v1"
	log "github.com/sirupsen/logrus"
	"os"
	"os/exec"
)

// SysDb 库sys.db 操作封装
type SysDb struct {
}

// Init 初始化
// 初始化创建sys.db库,里面创建userChannelPosition和channelPosition两个bucket
// 场景: 启动时就调用这个函数
func (s *SysDb) Init() {
	boltDbLock.Lock()
	defer boltDbLock.Unlock()

	// 数据目录不存在则创建目录
	if !utils_v1.File().CheckFileExist(CfgBoltDb.DbFolder) {
		exec.Command("mkdir", CfgBoltDb.DbFolder).Run()
	}

	// 打开db
	dbHandle, err := bolt.Open(CfgBoltDb.DbFolder+"/"+CfgBoltDb.SysDbName, os.FileMode(os.O_RDWR), nil)
	if err != nil {
		log.Panic(err)
	}
	defer func() { _ = dbHandle.Close() }()

	if err := dbHandle.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("userChannelPosition")); err != nil {
			log.Panic(err)
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("channelPosition")); err != nil {
			log.Panic(err)
		}
		return nil
	}); err != nil {
		log.Panic(err)
	}
}

// PullGetUserPosition
// 从boltdb里读取记录的用户的channel已读取位置(bucketKey为0表示新bucket里还没有读取任何数据)
// 场景: 拉取用户数据，每次取数据时需要先读取已读数据位置
func (s *SysDb) PullGetUserPosition(dbHandle *bolt.DB, userName, channelName string) (retBucketName, retBucketKey string, retErr error) {
	// 读取
	if err := dbHandle.View(func(tx *bolt.Tx) error {
		// 状态桶里查询数据桶状态
		if bucketPosition := tx.Bucket([]byte("userChannelPosition")); bucketPosition == nil {
			retErr = errors.New("sys.db 里的 bucket userChannelPosition 不存在")
			log.Error(retErr)
			return nil
		} else {
			c := bucketPosition.Cursor()
			if k, v := c.Seek([]byte(userName + "-" + channelName)); k == nil {
				retErr = errors.New("sys.db里的userChannelPosition bucket里不存在" + userName + "-" + channelName)
				log.Error(retErr)
				return nil
			} else {
				if d, err := json_v1.JsonDecode(string(v)); err != nil {
					retErr = err
					log.Error(err)
					return nil
				} else {
					retBucketName, _ = json_v1.GetJsonString(d, "bucketName")
					retBucketKey, _ = json_v1.GetJsonString(d, "bucketKey")
					return nil
				}
			}
		}
	}); err != nil {
		retErr = err
		log.Error(err)
	}
	return
}

// PullWriteUserPosition 写入用户当前用户的bucketName和bucketKey的值
// 直接修改boltdb数据表数据
// 场景: 数据成功发送给用户后，调用这个接口刷新数据发送位置
func (s *SysDb) PullWriteUserPosition(dbHandle *bolt.DB, userName, channelName, bucketName, bucketKey string) (retErr error) {
	if err := dbHandle.Update(func(tx *bolt.Tx) error {
		if bucketPosition := tx.Bucket([]byte("userChannelPosition")); bucketPosition == nil {
			retErr = errors.New("sys.db 里的 bucket userChannelPosition 不存在")
			log.Error(retErr)
			return nil
		} else {
			// 刷新位置
			d, _ := json_v1.JsonEncode(map[string]string{
				"bucketName":     bucketName,
				"bucketKey":      bucketKey,
				"lastActiveTime": utils_v1.Time().DateTime(),
			})
			if err := bucketPosition.Put([]byte(userName+"-"+channelName), []byte(d)); err != nil {
				retErr = err
				log.Error(err)
				return nil
			}
		}
		return nil
	}); err != nil {
		log.Panic(err)
	}

	return nil
}

// PullGetChannelPositionNow 获取频道当前位置
// 直接从boltdb里读取频道最新数据位置
// 场景: 用户消费数据时，指定了从最新位置读取时，将调用次函数
func (s *SysDb) PullGetChannelPositionNow(dbHandle *bolt.DB, channelName string) (retBucketName, retBucketKey string, retErr error) {
	// 读取
	if err := dbHandle.View(func(tx *bolt.Tx) error {
		// 状态桶里查询数据桶状态
		if b := tx.Bucket([]byte("channelPosition")); b == nil {
			retErr = errors.New("sys.db里不存在 channelPosition bucket")
			log.Error(retErr)
			return nil
		} else {
			c := b.Cursor()
			if k, v := c.Seek([]byte(channelName)); k == nil {
				log.Info("sys.db里的channelPosition bucket里不存在" + channelName)
				retBucketName = "0"
				retBucketKey = "0"
			} else {
				if d, err := json_v1.JsonDecode(string(v)); err != nil {
					retErr = err
					log.Error(err)
					return nil
				} else {
					retBucketName, _ = json_v1.GetJsonString(d, "bucketName")
					retBucketKey, _ = json_v1.GetJsonString(d, "bucketKey")
				}
			}
		}
		return nil
	}); err != nil {
		retErr = err
		log.Error(err)
	}
	return
}

// PullGetChannelPositionAll 获取频道最老位置
// 直接从boltdb里读取频道最老位置
// 场景: 用户消费数据时，指定了从最老位置读取时
func (s *SysDb) PullGetChannelPositionAll(dbHandle *bolt.DB, channelName string) (retBucketName, retBucketKey string, retErr error) {
	// 读取
	if err := dbHandle.View(func(tx *bolt.Tx) error {
		// 状态桶里查询数据桶状态
		if b := tx.Bucket([]byte("channelBuckets-" + channelName)); b == nil {
			retErr = errors.New("sys.db里不存在 channelBuckets-" + channelName)
			log.Error(retErr)
			return nil
		} else {
			c := b.Cursor()
			if k, _ := c.First(); k != nil {
				retBucketName = string(k)
				retBucketKey = "0"
			} else {
				retBucketName = "0"
				retBucketKey = "0"
			}
		}
		return nil
	}); err != nil {
		retErr = err
		log.Error(err)
	}
	return
}

// PushRefreshChannelPosition 写入数据后刷新频道的bucket集合 和 频道当前激活的bucket位置
// 直接写入boltdb
// 场景: 频道数据写入完成后，将调用这个函数刷新频道信息
func (s *SysDb) PushRefreshChannelPosition(channelName, bucketName, bucketKey string) (retErr error) {
	// 打开db
	dbHandle, err := bolt.Open(CfgBoltDb.DbFolder+"/"+CfgBoltDb.SysDbName, os.FileMode(os.O_RDWR), nil)
	if err != nil {
		log.Panic(err)
	}
	defer func() { _ = dbHandle.Close() }()

	if err := dbHandle.Update(func(tx *bolt.Tx) error {
		if bucketPosition := tx.Bucket([]byte("channelPosition")); bucketPosition == nil {
			retErr = errors.New("bucket channelPosition 不存在")
			log.Error(retErr)
			return nil
		} else {
			// 获取channelPosition已存在channel信息
			var positionNow map[string]string = nil
			cur := bucketPosition.Cursor()
			if k, v := cur.Seek([]byte(channelName)); k != nil {
				if d, err := json_v1.JsonDecode(string(v)); err != nil {
					retErr = err
					log.Error(err)
					return nil
				} else {
					positionNow = make(map[string]string)
					for _k, _v := range d.(map[string]interface{}) {
						positionNow[_k] = _v.(string)
					}
				}
			}

			// 刷新位置
			d, _ := json_v1.JsonEncode(map[string]string{
				"bucketName":     bucketName,
				"bucketKey":      bucketKey,
				"lastActiveTime": utils_v1.Time().DateTime(),
			})
			if err := bucketPosition.Put([]byte(channelName), []byte(d)); err != nil {
				retErr = err
				log.Error(err)
				return nil
			}

			// 刷新频道bucket状态
			if err := s.refreshChannelBuckets(tx, channelName, bucketName, positionNow); err != nil {
				retErr = err
				log.Error(err)
				return nil
			}
		}

		// 二、处理 channelPosition 【频道当前激活的bucket】
		return nil
	}); err != nil {
		log.Panic(err)
	}
	return retErr
}

// 刷新频道bucket状态
// 直接写入boltdb
// 场景: 频道数据写入完成后，刷新频道位置里将调用这个函数刷新频道信息
func (s *SysDb) refreshChannelBuckets(tx *bolt.Tx, channelName string, bucketName string, positionNow map[string]string) error {
	// 关闭激活状态
	if bucket, err := tx.CreateBucketIfNotExists([]byte("channelBuckets-" + channelName)); err != nil {
		log.Error(err)
		return err
	} else {
		if positionNow != nil && positionNow["bucketName"] != bucketName {
			if err := bucket.Put([]byte(positionNow["bucketName"]), []byte("0")); err != nil {
				log.Error(err)
				return err
			}
		}

		// 开启激活状态
		if err := bucket.Put([]byte(bucketName), []byte("1")); err != nil {
			log.Error(err)
			return err
		}
	}
	return nil
}
