package main

import (
	"flag"
	"fmt"
	"github.com/loudbund/go-mq/config"
	"github.com/loudbund/go-mq/service"
	"github.com/loudbund/go-progress/progress_v1"
	log "github.com/sirupsen/logrus"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"strings"
)

// 初始化
func init() {
	// 设置全局日志配置
	log.SetReportCaller(true)
}

var version = "200012125959"

func init() {
	// 设置全局日志配置
	log.SetReportCaller(true)
	// 设置自定义 Formatter, 修改日志里代码文件相对路径
	log.SetFormatter(&log.TextFormatter{
		CallerPrettyfier: func(f *runtime.Frame) (string, string) {
			file := strings.Split(f.File, "go-mq")
			return fmt.Sprintf("%s:%d", file[len(file)-1], f.Line), ""
		},
	})
	log.SetLevel(log.Level(config.CfgServer.LogLevel))

}

// 程序主入口
func main() {
	fmt.Println("app version:", version)

	var c *string // 运行方式
	flag.String("v", "", "")
	c = flag.String("c", "run", "运行方式(status/run/start/stop/kill/view)")
	flag.Parse()

	// 查看数据状态
	if *c == "view" {
		(&service.Controller{}).View()
		return
	}

	// 运行
	progress_v1.Exec(Exec, *c, "logs/log.txt")
}

// Exec 程序正式入口
func Exec() {
	// 启动grpc服务
	go service.RunGrpcServer(config.CfgServer.Host, config.CfgServer.Port)

	// 启动pprof
	if config.CfgServer.PprofPort > 0 {
		pprofAddr := fmt.Sprintf("0.0.0.0:%d", config.CfgServer.PprofPort)
		go func(addr string) {
			if err := http.ListenAndServe(addr, nil); err != http.ErrServerClosed {
				log.Fatalf("Pprof server ListenAndServe: %v", err)
			}
		}(pprofAddr)
		log.Infof("HTTP Pprof start at %s", pprofAddr)
	}

	select {}
}
