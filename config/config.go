package config

import (
	"embed"
	"encoding/json"
	"github.com/loudbund/go-utils/utils_v1"
	log "github.com/sirupsen/logrus"
	"gopkg.in/ini.v1"
)

var CfgServer = struct {
	// 监听ip
	Host string `ini:"Host"`
	// 监听端口
	Port int `ini:"Port"`

	// pprof监听端口, 0表示不监听
	PprofPort int `ini:"PprofPort"`

	// 数据单次pull行数
	DataRowPerPull int `ini:"DataRowPerPull"`

	// 数据保留小时数
	HourDataRetain int `ini:"HourDataRetain"`

	// 数据所在目录
	DataDirName string `ini:"DataDirName"`

	// 日志级别
	LogLevel uint32 `ini:"LogLevel"`

	// 拉取数据的用户名和密码
	Username string `ini:"PullUsername"`
	Password string `ini:"PullPassword"`
}{}

//go:embed embed.app.conf
var fileList embed.FS

func init() {
	// 生成目录
	if !utils_v1.File().CheckFileExist("conf") {
		_ = utils_v1.File().MkdirAll(".", "conf")
	}
	if !utils_v1.File().CheckFileExist("logs") {
		_ = utils_v1.File().MkdirAll(".", "logs")
	}

	// 生成example配置文件
	if data, err := fileList.ReadFile("embed.app.conf"); err == nil {
		_ = utils_v1.File().WriteAll("conf/example.app.conf", data)
	}

	// 加载配置文件、并映射到结构体变量
	c1, err := ini.Load("conf/config.app.conf")
	if err != nil {
		log.Fatalf("Error reading config file, %s\n", err)
	}
	err = c1.MapTo(&CfgServer)
	if err != nil {
		log.Fatalf("Error parsing config file, %s\n", err)
	}

	// 数据目录还不存在，则创建一个
	CfgServer.DataDirName = "boltDb"
	if !utils_v1.File().CheckFileExist(CfgServer.DataDirName) {
		_ = utils_v1.File().MkdirAll(".", CfgServer.DataDirName)
	}

	log.Infof("配置参数:%v", jsonMarshal(CfgServer))
}

func jsonMarshal(v interface{}) string {
	cf, _ := json.Marshal(v)
	return string(cf)
}
