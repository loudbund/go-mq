package service

import (
	"github.com/loudbund/go-utils/utils_v1"
	log "github.com/sirupsen/logrus"
	ini "gopkg.in/ini.v1"
)

var CfgBoltDb = struct {
	// 监听ip
	Host string `ini:"Host"`
	// 监听端口
	Port int `ini:"Port"`

	// 数据单次pull行数
	DataRowPerPull int `ini:"DataRowPerPull"`

	// 数据保留小时数
	HourDataRetain int `ini:"HourDataRetain"`

	// 数据所在目录
	DataDirName string `ini:"DataDirName"`

	// 日志级别
	LogLevel uint32 `ini:"LogLevel"`
}{}

func init() {
	// 初始化CfgBoltDb变量配置
	// 加载配置文件
	c1, err := ini.Load("conf/app.conf")
	if err != nil {
		log.Fatalf("Error reading config file, %s\n", err)
	}

	// 影射到结构体变量
	err = c1.MapTo(&CfgBoltDb)
	if err != nil {
		log.Fatalf("Error parsing config file, %s\n", err)
	}

	// 数据目录还不存在，则创建一个
	CfgBoltDb.DataDirName = "boltDb"
	if !utils_v1.File().CheckFileExist(CfgBoltDb.DataDirName) {
		utils_v1.File().MkdirAll(".", CfgBoltDb.DataDirName)
	}
}
