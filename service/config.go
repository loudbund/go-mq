package service

import (
	"github.com/loudbund/go-utils/utils_v1"
	log "github.com/sirupsen/logrus"
	ini "gopkg.in/ini.v1"
)

var CfgBoltDb = struct {
	// 监听ip
	Host string
	// 监听端口
	Port int

	//存储是否开启zlib，默认不开启
	DBZlibOpen bool
	//拉取是否开启zlib，默认不开启
	PullZlibOpen bool

	// 数据单次获取最大行数
	DataMaxRowCurGet int
	// 数据单次pull行数
	DataRowCurPull int

	// 数据保留小时数
	HourDataRetain int

	// 数据所在目录
	DataDirName string
}{}

func init() {
	// app.conf配置
	c1, err := ini.Load("conf/app.conf")
	if err != nil {
		log.Fatalf("Error reading config file, %s\n", err)
	}

	// 配置变量写入结构体变量
	err = c1.MapTo(&CfgBoltDb)
	if err != nil {
		log.Fatalf("Error parsing config file, %s\n", err)
	}

	// 数据目录不存在则创建
	CfgBoltDb.DataDirName = "boltDb"
	if !utils_v1.File().CheckFileExist(CfgBoltDb.DataDirName) {
		utils_v1.File().MkdirAll(".", CfgBoltDb.DataDirName)
	}
}
