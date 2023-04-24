/**
 * @Author: hqd
 * @Date: 2023/4/21
 * @Description: consumer/main.go
 */

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/Shopify/sarama"
	"github.com/hqd8080/mq"
)

var (
	hosts   = []string{"127.0.0.1:9091"}
	topic   = "hqd8080"
	groupID = "test-group"
)

type Message struct {
	ID       int64  `json:"id"`
	Name     string `json:"name"`
	CreateAT string `json:"create_at"`
}

func main() {
	_, err := mq.StartConsumer(hosts, []string{topic}, groupID, nil, MessageHandler)
	if err != nil {
		fmt.Println("consumer message error:", err)
		return
	}
	signals := make(chan os.Signal)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	select {
	case s := <-signals:
		fmt.Println("kafka test receive system signal:", s)
		return
	}
}

func MessageHandler(message *sarama.ConsumerMessage) (bool, error) {
	fmt.Printf("consumer receive message: topic:%s, partition:%d, offset:%d, value:%s\n", message.Topic, message.Partition, message.Offset, string(message.Value))
	msg := &Message{}
	err := json.Unmarshal(message.Value, msg)
	if err != nil {
		fmt.Println("unmarshal message error:", err)
		return false, nil
	}
	fmt.Printf("message:%+v\n", msg)
	return true, nil
}
