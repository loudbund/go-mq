# go-request

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

## bolt库说明
```
|--	data.db
|	|--	bucket:Info		// 整体信息
|	|	|--	key:ChannelId		// JSON格式{"${频道名}":${频道id(int32)}}
|	|	|--	key:WritingInfo		// 当前写入bucket记录(数据写入用)
						// struct {
						//	BucketId int32	// 当前bucketid
						//	MaxSId	 int32	// 当前已写入的序号号id
						// }
|	|--	bucket:(时间int搓转byte) // [byte-int32],一个bucket存一个小时的数据
|	|	|--	key:BucketInfo			// bucket信息(数据读取用)
						// struct {
						//	MaxSId 		 int32	  // 写入的最大序列号id[byte-bint32],为-1表示还未写入结束
						// }
|	|	|--	key:[byte-bint32]	// 序列号id对应明细数据，从1开始【其值前4个字节为频道id】
```

## 数据写入
- 参数

  ```
  - channelName(string)	频道名
  - data([]byte)			内容数据
  ```

- 流程

  - 获取频道id
    - 判断内存变量里是否存在这个频道
    - 没有，则新增一个，并整体刷新data.db的key:channels
    - 有，则取出频道id并转换成byte
  - 取出当前的数据bucket名，序列号id，
  - 频道id和数据整合
  - 数据写入
  - 刷新下一数据bucket名和序列号

## 数据读取

- 参数

  ```
  -- postion 已获取了的位置，[byte-int32][byte-int32](数据bucket的id和序列号id)
  ```

- 返回

  ```
  一次返回多条数据
  []struct{
  	Position:([byte-int32][byte-int32])//单条数据的bucketid和序列号id[byte-int32][byte-int32]
  	Channel:(string) // 频道名称(为空，表示这是一条定位数据)
  	Data:[]byte // 数据内容
  }
  ```

- 逻辑

  - 打开数据bucket
  - 判断数据是否已全部读取
    - 已全部读取且没有下一bucket标记，返回
    - 已全部读取且有下一bucket标记，去读取下一bucket数据
    - 未全部读取，持续读取


