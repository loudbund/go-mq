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

## 需要解决的问题

- 问题
  - 压缩数据对于磁盘和流量有益处，但是消耗了cpu性能
- 场景分析
  - 局域网内部带宽大，可以不用考虑流量消耗
  - 跨机房需要考虑流量带宽，所以传输前需要数据压缩
  - 大部分连接是近实时的
- 方案
  - 近几个小时的数据采用为压缩存储，
    - 大部分客户端都是从这里面取数据的
    - 局域网客户端直接不压缩传输
    - 跨机房采用整体压缩
      - 通过增加延时请求，可以更大的提高聚合量
  - 存量数据高聚合压缩存储
    - 减少了磁盘消耗
    - 整体将压缩数据发给客户端

## boltDb库说明
```
|--	data.db
|	|--	桶1:	Run				// 运行信息,名称共3个字节
|	|	|--	键:TopicMap			// 频道和频道id的map数据
						// 键: TopicMap
						// 值: JSON格式{"${频道名}":${频道id(int32)}}
|	|	|--	键:Writing			// 当前写入bucket记录(数据写入用)
						// 键: Writing
						// 值:
						// struct {
						//	Bucket int32	// 当前bucket
						//	DataId int32	// 当前已写入的序号号id
						// }
|	|--	桶2:(时间int搓转byte) // 一个bucket存一个小时的未压缩数据,名称共4个字节
|	|	|--	键:Header			// bucket信息(数据读取用),
						// 键: Header，键共6个字节
						// 值:
						// struct {
						//	MaxDataId 		 int32	  // bucket内的最大数据id[byte-bint32],0表示写入还未结束
						// }
|	|	|--	键:[int32转]			// bucket内的数据信息,4个字节
						// 键: bucket内数据id,从1开始，键共4个字节，
						// 值: 其值前4个字节[0:4]为频道id，后4个字节[4:8]为数据id
|	|	|--	键:[int32转]			// bucket内的数据内容，3个字节
						// 键: bucket内数据id,从1开始，键共3个字节，和数据信息的键id值是一样的，只是丢掉1位高字节
						// 值: 第1个字节为数据是否压缩，第2个开始为具体的数据内容
						// 内容结构体为 struct{
						//	DataId	int32
						//	TopicId	int32
						//	Data	[]byte
						// }
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
  一次返回多条数据,数据不跨bucket
  []struct{
  	Position:([byte-int32][byte-int32])//单条数据的bucket和数据id[byte-int32][byte-int32]
  	TopicMap:(map) 	// 数据包含的频道id和名称(格式为 map[int32]string)
  	Zlib:(bool)		// Data数据结构，
  	Data:[]byte 		// 数据内容(多条)
  			// Zlib==false,整体未压缩，压缩前格式为[]struct{ TopicId int32, DataId int32, Data []byte}
  			// Zlib==true,整体压缩，压缩前格式为[]struct{ TopicId int32, DataId int32, Data []byte}
  }
  ```

- 逻辑

  - 1、指针位置比写入中的最新位置还新，返回指针位置

  - 2、bucket等于正在写入中的bucket，则从写入中的bucket里取，并返回

    - 循环取一批数据
    - 取出的数据整理并返回
    - 没有新数据返回指针位置

  - 3、循环从未压缩的bucket取，

    - 指针bucket和写入中的bucket相等，跳出循环
    - 数据量符合要求，跳出循环
    - 当前bucket未取完，且已经取过数据，则再取一条
    - 当前bucket位置为0
      - 判断bucket是否存在，存在，则标记只能从未压缩bucket里取，并取出一条数据
      - 不存在，且未标记只能从未压缩的bucket里取数据，则从压缩bucket里取数据
        - 压缩bucket里取到了数据，则处理并直接返回

  - 4、处理数据并返回

    
