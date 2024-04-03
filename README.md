# go-request

## 使用说明
- 1、启动goMq.runcgo服务，需要先配置grpc端口号
- 2、消息生产者推送消息给服务，参见client-example/push
- 3、消息消费者推送消息给服务，参见client-example/pull

## bolt库说明
- 1、库sys.db
  - bucket:  userChannelPosition             【用户拉取数据当前频道拉取位置】
    - ${userid}-${channelName} = {"bucketName":"xxx","bucketKey":"xxx","lastActiveTime":"2024-01-01 12:12:12"}
  - bucket:  channelBuckets-${channelName}  【频道的bucket集合】
    - ${bucketName} = 1/0        (1:写入激活 0:写入关闭)
  - bucket:  channelPosition                     【频道当前激活的bucket】
    - ${channelName} = {"bucketName":"xxx","bucketKey":"xxx","lastActiveTime":"2024-01-01 12:12:12"}
- 2、${channelName}.db
  - bucket: ${bucketName}
    - ${bucketKey} = ${Data}

## 数据读取
- 1、拉取位置判断
  - 指定读取最新的：直接取channelPosition的当前位置
    - read  < sys.db / channelPosition
  - 参数指定拉取位置：判断位置是否还存在
    - read  < sys.db / channelBuckets-${channelName}
  - 未指定拉取位置：读取用户标记的已读取
    - read  < sys.db / userChannelPosition
  - 未指定拉取位置：直接取channelPosition的当前位置
    - read  < sys.db / channelPosition
- 2、读取到指定条数或大小，读取结束
  - read  < ${channelName}.db / ${bucketName}
- 3、已读取完一个历史bucket，切换到下个bucket继续读取
  - read  < sys.db / channelBuckets-${channelName}
- 4、激活的bucket已读取完，读取结束
- 5、读取结束后刷新用户读取位置
  - write < sys.db / userChannelPosition

## 数据写入
- 1、矫正频道当前的激活bucket并返回激活的bucket
  - read  < sys.db / channelPosition
  - write > sys.db / channelPosition
  - write > sys.db / channelBuckets-${channelName}
- 2、写入一批数据
  - write > ${channelName}.db / ${bucketName}
- 3、刷新频道当前激活bucket信息
  - write > sys.db > channelPosition

### 使用说明




