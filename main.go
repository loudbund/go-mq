package main

import (
	"embed"
	"flag"
	"fmt"
	"github.com/loudbund/go-mq/service"
	"github.com/loudbund/go-progress/progress_v1"
	"github.com/loudbund/go-utils/utils_v1"
	log "github.com/sirupsen/logrus"
	_ "net/http/pprof"
)

// 初始化
func init() {
	// 设置全局日志配置
	log.SetReportCaller(true)
}

var version = "200012125959"

//go:embed conf/embed.app.conf
var fileList embed.FS

func init() {
	// 设置全局日志配置
	log.SetReportCaller(true)
	// 生成example配置文件
	if !utils_v1.File().CheckFileExist("conf") {
		utils_v1.File().MkdirAll(".", "conf")
	}
	if data, err := fileList.ReadFile("conf/embed.app.conf"); err == nil {
		utils_v1.File().WriteAll("conf/example.app.conf", data)
	}
	// 生成日志目录
	if !utils_v1.File().CheckFileExist("logs") {
		utils_v1.File().MkdirAll(".", "logs")
	}

	// 读取配置
	service.CfgBoltDb.Host, _ = utils_v1.Config().GetCfgString("conf/app.conf", "main", "Host")
	service.CfgBoltDb.Port, _ = utils_v1.Config().GetCfgInt("conf/app.conf", "main", "Port")

	service.CfgBoltDb.DbFolder, _ = utils_v1.Config().GetCfgString("conf/app.conf", "main", "DbFolder")
	service.CfgBoltDb.SysDbName, _ = utils_v1.Config().GetCfgString("conf/app.conf", "main", "SysDbName")
	service.CfgBoltDb.PreChannelDb, _ = utils_v1.Config().GetCfgString("conf/app.conf", "main", "PreChannelDb")

	service.CfgBoltDb.PushPerNumLimit, _ = utils_v1.Config().GetCfgInt("conf/app.conf", "main", "PushPerNumLimit")

	service.CfgBoltDb.BucketNameUserChannelPosition, _ = utils_v1.Config().GetCfgString("conf/app.conf", "main", "BucketNameUserChannelPosition")
	service.CfgBoltDb.BucketNameChannelPosition, _ = utils_v1.Config().GetCfgString("conf/app.conf", "main", "BucketNameChannelPosition")

	service.CfgBoltDb.HourDataRetain, _ = utils_v1.Config().GetCfgInt("conf/app.conf", "main", "HourDataRetain")

	fmt.Println(service.CfgBoltDb)

	// 初始化系统数据库
	(&service.SysDb{}).Init()
}

// 程序主入口
func main() {
	fmt.Println("app version:", version)

	var c *string // 运行方式
	flag.String("v", "", "")
	c = flag.String("c", "run", "运行方式(status/run/start/stop/kill)")
	flag.Parse()

	// 运行
	progress_v1.Exec(Exec, *c, "logs/log.txt")
}

// Exec 程序正式入口
func Exec() {
	// 启动grpc服务
	go service.RunGrpcServer(service.CfgBoltDb.Host, service.CfgBoltDb.Port)

	// 启动定时清理
	go service.RunClearExpireBucket()

	// 启动pprof
	//pprofAddr := "0.0.0.0:6060"
	//go func(addr string) {
	//	if err := http.ListenAndServe(addr, nil); err != http.ErrServerClosed {
	//		log.Fatalf("Pprof server ListenAndServe: %v", err)
	//	}
	//}(pprofAddr)
	//log.Infof("HTTP Pprof start at %s", pprofAddr)

	select {}
}
