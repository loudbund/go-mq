# go-mq

## 使用说明
- 1、启动goMq.runcgo服务，需要先配置grpc端口号
- 2、消息生产者推送消息给服务，参见client-example/push
- 3、消息消费者推送消息给服务，参见client-example/pull

## 特点
- 1、golang语言开发，占用系统资源极少
- 2、使用boltdb存储队列数据，数据持久化存储
- 3、数据保留时间可配置，配置变量HourDataRetain
- 4、提供了数据生成者和消费者的golang语言版本的sdk
- 5、客户端sdk可能会缓存最长1s数据；消费数据时队列为空时，将延时1s。所以在无存量数据时，最长可能延时2s

- 6、结构最简化，数据也不作压缩，数据压缩交给业务自己处理

## boltDb库说明
```
|--	data.db
|	|--	桶1:	Set				// 运行信息,名称共3个字节
|	|	|--	键:TopicMap			// 频道和频道id的map数据
						// 键: TopicMap
						// 值: JSON格式{"${频道名}":${频道id(int32)}}
|	|--	桶2:(时间int搓转byte) // 一个bucket存一个小时的未压缩数据,名称共4个字节
|	|	|--	键:MaxDataId			// bucket内的最大数据id[byte-bint32],未写入结束不产生该键
|	|	|--	键:[int32转]			// bucket内的数据详情，4个字节
						// 键: bucket内数据id,键值从1开始，键共4个字节
						// 值: 前4个字节为频道id，其后为数据内容
```

## 数据写入
- 参数

  ```
  - topic(string)			频道名
  - data([]byte)			内容数据
  ```

- 流程

  - 获取频道id
    - 判断内存变量里是否存在这个频道
    - 没有，则新增一个，并整体刷新data.db的key:TopicsIds
    - 有，则取出频道id并转换成byte
  - 取出当前的数据bucket名，序列号id，
  - 频道id和数据整合
  - 数据写入
  - 刷新下一数据bucket名和序列号

## 数据读取

- 参数

  ```
  -- postion 已获取了的位置，[byte-int32][byte-int32](数据bucket的id和数据id)
  ```

- 返回

  ```
  struct{
  	Position:([byte-int32][byte-int32])//单条数据的bucket和数据id[byte-int32][byte-int32]
  	Topic:(string) 	// 频道名称
  	Data:[]byte 		// 数据内容
  }
  ```
  
  
  
