package service

var CfgBoltDb = struct {
	Host string // 监听ip，
	Port int    // 监听端口

	DbFolder     string // 数据库存放文件夹
	SysDbName    string // 系统数据库名称
	PreChannelDb string // 频道数据前缀名

	PushPerNumLimit int // 单次推送条数限制，这个条数内需要有一条WaitRes的数据触发写入

	BucketNameUserChannelPosition string // 用户拉取数据当前频道拉取位置的bucket名(系统数据库里)
	BucketNameChannelPosition     string // 频道当前激活的bucket(系统数据库里)

	HourDataRetain int // 数据保留小时数
}{}
