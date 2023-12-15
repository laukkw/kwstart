package kwfka

import (
    "github.com/laukkw/kwstart/errors"
    "github.com/segmentio/kafka-go"
    "sync"
    "time"
)

// writer create
var (
    kafkaWriter *kafka.Writer
    onceWriter  sync.Once
)

func GetKafakaWriter(hosts []string) (*kafka.Writer, error) {
    onceWriter.Do(func() {
        getWrite(hosts)
    })
    if kafkaWriter == nil {
        return nil, errors.New("create kafka error,check your broker")
    }
    return kafkaWriter, nil
}

func GetKafakaReader(hosts []string, groupId string, topic string) *kafka.Reader {
    return kafka.NewReader(kafka.ReaderConfig{
        Brokers:          hosts,
        GroupID:          groupId,
        Topic:            topic,
        MinBytes:         10e3, // 10KB
        MaxBytes:         10e6, // 10MB
        MaxWait:          time.Second * 1,
        ReadBatchTimeout: 500 * time.Millisecond,
        CommitInterval:   100 * time.Millisecond, // 自动提交offset的时间间隔
    })
}

func getWrite(hosts []string) {
    // 所有 ReplicationFactor 都确认才算完成.
    // key选择使用 leastBytes . 相同的key 存在相同的分区. 保持有序.
    // 所以在插入的时候. 如果是一个系列的 就设置同样的key.
    kafkaWriter = &kafka.Writer{
        Addr:         kafka.TCP(hosts...),
        Topic:        "",
        Balancer:     &kafka.LeastBytes{},
        RequiredAcks: kafka.RequireAll,
        BatchSize:    1,
        // 负责在Kafka集群 内对partition进行重新分配和平衡。.会让不同broke平衡
        /*    MaxAttempts:            0,                   // 重试次数
              WriteBackoffMin:        0,                   // 重试间最小和最大回退时间
              WriteBackoffMax:        0,
              BatchSize:              0, // 批量发送的消息数
              BatchBytes:             0, // 批量发送的最大数据大小
              BatchTimeout:           0, // 批量发送的超时时间
              ReadTimeout:            0, //
              WriteTimeout:           0,
              RequiredAcks:           0,     // 所需 分区的确认数 . 和kafka 一样. 1 0  all/-1
              Async:                  false, // 是否启用异步发送
              Completion:             nil,   // 消息发送完成回调函数。
              Compression:            0,     // 消息压缩方式。
              Logger:                 nil,   // 自定义日志记录器。
              ErrorLogger:            nil,   // 错误日志记录器。
              Transport:              nil,   // 自定义传输层
              AllowAutoTopicCreation: false, // 是否允许自动创建Topic。 */
    }
    return
}
