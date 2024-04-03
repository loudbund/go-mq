package main

import (
	"flag"
	"fmt"
	"github.com/loudbund/go-mq/service"
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

func init() {
	// 打印版本
	fmt.Println("app version:", version)

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
	flag.String("v", "", "")
	flag.Parse()

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
