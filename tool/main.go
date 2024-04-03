package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/boltdb/bolt"
	"github.com/loudbund/go-utils/utils_v1"
	log "github.com/sirupsen/logrus"
	"os"
)

// 6、主函数 -------------------------------------------------------------------------
func main() {
	// 1、参数获取
	var Db, Bucket *string // 运行方式
	Db = flag.String("db", "", "数据库文件地址")
	Bucket = flag.String("bucket", "", "bucket名称")
	flag.Parse()

	if *Db == "" {
		log.Panic("参数db是必需的")
	}

	// 判断数据库文件是否存在
	if !utils_v1.File().CheckFileExist(*Db) {
		log.Panic("数据库文件 " + *Db + " 不存在")
	}

	// 打开db
	dbHandle, err := bolt.Open(*Db, os.FileMode(os.O_RDONLY), nil)
	if err != nil {
		log.Panic(err)
	}
	defer func() { _ = dbHandle.Close() }()

	// 没有Bucket参数，则读取bucket列表，否则读取bucket里所有数据(1000条数据)
	if *Bucket == "" {
		// 读取bucket列表
		if err := dbHandle.View(func(tx *bolt.Tx) error {
			if err := tx.ForEach(func(name []byte, b *bolt.Bucket) error {
				fmt.Println(string(name))
				return nil
			}); err != nil {
				log.Panic(err)
			}
			return nil
		}); err != nil {
			log.Panic(err)
		}

	} else {
		// 读取bucket里的数据
		if err := dbHandle.View(func(tx *bolt.Tx) error {
			if bucket := tx.Bucket([]byte(*Bucket)); bucket == nil {
				log.Panic(errors.New("bucket " + *Bucket + " 不存在"))
			} else {
				if cur := bucket.Cursor(); cur == nil {
					log.Panic(errors.New("获取cursor游标失败"))
				} else {
					n := 0
					for k, v := cur.Last(); k != nil; k, v = cur.Prev() {
						fmt.Println(string(k), string(v))
						n++
						if n > 1000 {
							fmt.Println("more than 1000 row, ignore other")
							break
						}
					}
				}
			}

			return nil
		}); err != nil {
			log.Panic(err)
		}
	}

}
