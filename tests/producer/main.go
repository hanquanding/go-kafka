/**
 * @Author: hqd
 * @Date: 2023/4/21
 * @Description: producer/main.go
 */

package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/hqd8080/mq"
)

var (
	hosts = []string{"127.0.0.1:9091"}
	topic = "hqd8080"
)

type Message struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	CreateAT string `json:"create_at"`
}

func main() {
	err := mq.InitSyncProducer(mq.DefaultSyncProducer, hosts, nil)
	if err != nil {
		fmt.Println("init sync kafka producer error:", err)
		return
	}
	msg := Message{
		ID:       1,
		Name:     "test!!",
		CreateAT: time.Now().Format(time.RFC3339),
	}
	messageBody, err := json.Marshal(msg)
	if err != nil {
		fmt.Println("marshal message error:", err)
		return
	}
	producerMessage := &sarama.ProducerMessage{
		Topic: topic,
		Value: mq.MessageValueByteEncoder(messageBody),
	}
	partition, offset, err := mq.GetSyncProducer(mq.DefaultSyncProducer).SendMessage(producerMessage)
	if err != nil {
		fmt.Println("sync send message error:", err)
		return
	}
	fmt.Printf("sync send message success partition:%d, offset:%d\n", partition, offset)
}
