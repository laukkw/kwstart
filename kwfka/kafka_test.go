package kwfka

import (
    "context"
    "fmt"
    "github.com/segmentio/kafka-go"
    "testing"
    "time"
)

func TestGetKafakaWriter(t *testing.T) {
    hosts := []string{"localhost:9092", "localhost:9093", "localhost:9094"}
    writer, err := GetKafakaWriter(hosts)
    if err != nil {
        t.Fatal(err)
    }
    // test write message
    // i := 0
    defer writer.Close()
    for i := 0; i <= 10; i++ {
        err = writer.WriteMessages(context.Background(), kafka.Message{
            Topic: "transfer",
            Value: []byte(fmt.Sprintf("p1 .this is key %d  ", i)),
            Key:   []byte(fmt.Sprintf("transfer-%d", i)),
        })
        if err != nil {
            t.Fatal(err)
        }
        t.Log("send ok ->", i)
        time.Sleep(1 * time.Second)
    }
}

// 47.242.148.138:19092
func TestGetKafakaReader(t *testing.T) {
    // hosts := []string{"localhost:9092", "localhost:9093", "localhost:9094"}
    hosts := []string{"47.242.148.138:19092"}
    r := GetKafakaReader(hosts, "msg-to", "transfer")

    defer r.Close()
    for {
        messages, err := r.ReadMessage(context.Background())
        if err != nil {
            t.Fatal(err)
        }

        switch string(messages.Key) {
        case fmt.Sprintf("transfer-%d", 0):
            t.Logf("当前输入,仓库为 %v ,钥匙为 : %v,偏移量为 : %v", messages.Partition, string(messages.Key), messages.Offset)
        case fmt.Sprintf("transfer-%d", 1):
            t.Logf("当前输入,仓库为 %v ,钥匙为 : %v,偏移量为 : %v", messages.Partition, string(messages.Key), messages.Offset)
        case fmt.Sprintf("transfer-%d", 2):
            t.Logf("当前输入,仓库为 %v ,钥匙为 : %v,偏移量为 : %v", messages.Partition, string(messages.Key), messages.Offset)
        case fmt.Sprintf("transfer-%d", 3):
            t.Logf("当前输入,仓库为 %v ,钥匙为 : %v,偏移量为 : %v", messages.Partition, string(messages.Key), messages.Offset)
        case fmt.Sprintf("transfer-%d", 4):
            t.Logf("当前输入,仓库为 %v ,钥匙为 : %v,偏移量为 : %v", messages.Partition, string(messages.Key), messages.Offset)
        case fmt.Sprintf("transfer-%d", 5):
            t.Logf("当前输入,仓库为 %v ,钥匙为 : %v,偏移量为 : %v", messages.Partition, string(messages.Key), messages.Offset)
        case fmt.Sprintf("transfer-%d", 6):
            t.Logf("当前输入,仓库为 %v ,钥匙为 : %v,偏移量为 : %v", messages.Partition, string(messages.Key), messages.Offset)
        case fmt.Sprintf("transfer-%d", 7):
            t.Logf("当前输入,仓库为 %v ,钥匙为 : %v,偏移量为 : %v", messages.Partition, string(messages.Key), messages.Offset)
        case fmt.Sprintf("transfer-%d", 8):
            t.Logf("当前输入,仓库为 %v ,钥匙为 : %v,偏移量为 : %v", messages.Partition, string(messages.Key), messages.Offset)
        case fmt.Sprintf("transfer-%d", 9):
            t.Logf("当前输入,仓库为 %v ,钥匙为 : %v,偏移量为 : %v", messages.Partition, string(messages.Key), messages.Offset)
        case fmt.Sprintf("transfer-%d", 10):
            t.Logf("当前输入,仓库为 %v ,钥匙为 : %v,偏移量为 : %v", messages.Partition, string(messages.Key), messages.Offset)
        default:
            t.Logf("--")
        }

    }

}
