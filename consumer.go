package mq

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/eapache/go-resiliency/breaker"
	"github.com/pkg/errors"
)

const (
	ConsumerConnected    string = "connected"
	ConsumerDisconnected string = "disconnected"
)

// KafkaConsumer 消费者信息
type KafkaConsumer struct {
	hosts      []string
	topics     []string
	config     *cluster.Config
	consumer   *cluster.Consumer
	status     string
	groupID    string
	breaker    *breaker.Breaker
	reConnect  chan bool
	statusLock sync.Mutex
	done       bool
}

// MessageHandler 消费者回调函数
type MessageHandler func(message *sarama.ConsumerMessage) (bool, error)

// GetDefaultConsumerConfig 获取默认消费者配置
func GetDefaultConsumerConfig() (config *cluster.Config) {
	config = cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Group.Session.Timeout = time.Second * 20
	config.Consumer.Group.Heartbeat.Interval = time.Second * 6
	config.Consumer.MaxProcessingTime = time.Millisecond * 500
	config.Consumer.Fetch.Default = 1024 * 1024 * 2
	config.Group.Return.Notifications = true
	config.Version = sarama.V2_0_0_0
	return
}

// StartConsumer 启动消费者消费
func StartConsumer(hosts, topics []string, groupID string, config *cluster.Config, f MessageHandler) (*KafkaConsumer, error) {
	var err error
	if config == nil {
		config = GetDefaultConsumerConfig()
	}
	consumer := &KafkaConsumer{
		hosts:      hosts,
		topics:     topics,
		config:     config,
		status:     ConsumerDisconnected,
		groupID:    groupID,
		breaker:    breaker.New(3, 1, time.Second*3),
		reConnect:  make(chan bool),
		statusLock: sync.Mutex{},
		done:       false,
	}
	if consumer.consumer, err = cluster.NewConsumer(hosts, groupID, topics, consumer.config); err != nil {
		return consumer, err
	}
	consumer.status = ConsumerConnected
	log.Printf("kafka consumer started, groupId:%s, topics:%v\n", groupID, topics)
	go consumer.keepConnect()
	go consumer.consumerMessage(f)
	return consumer, err
}

// consumerMessage 消费者消费消息
func (c *KafkaConsumer) consumerMessage(f MessageHandler) {
	for !c.done {
		if c.status != ConsumerConnected {
			time.Sleep(time.Second * 5)
			log.Printf("kafka consumer status: [%s]\n", c.status)
			continue
		}
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		go func() {
			for notify := range c.consumer.Notifications() {
				log.Printf("kafka consumer rebalanced: groupID:%s, notify:%v\n", c.groupID, notify)
			}
		}()
	consumerLoop:
		for !c.done {
			select {
			case msg, ok := <-c.consumer.Messages():
				if ok {
					if commit, err := f(msg); commit {
						c.consumer.MarkOffset(msg, "") // mark message as processed
					} else {
						if err != nil {
							log.Printf("kafka consumer message error:%v\n", err)
						}
					}
				}
			case err := <-c.consumer.Errors():
				log.Printf("kafka consumer message error:%v\n", err)
				if errors.Is(err, sarama.ErrOutOfBrokers) || errors.Is(err, sarama.ErrNotConnected) {
					c.statusLock.Lock()
					if c.status == ConsumerConnected {
						c.status = ConsumerDisconnected
						c.reConnect <- true
					}
					c.statusLock.Unlock()
				}
			case s := <-signals:
				log.Printf("kafka consumer received signal:%s\n", s.String())
				c.statusLock.Lock()
				c.done = true
				if err := c.consumer.Close(); err != nil {
					log.Printf("kafka consumer close error:%v\n", err)
				}
				c.statusLock.Unlock()
				break consumerLoop
			}
		}
	}
}

// keepConnect 消费者保持断线重连
func (c *KafkaConsumer) keepConnect() {
	for !c.done {
		select {
		case <-c.reConnect:
			if c.status != ConsumerDisconnected {
				break
			}
			log.Printf("kafka consumer reconnecting, groupID:%s, topocs:%v\n", c.groupID, c.topics)
			var consumer *cluster.Consumer
		breakLoop:
			for {
				err := c.breaker.Run(func() (err error) {
					consumer, err = cluster.NewConsumer(c.hosts, c.groupID, c.topics, c.config)
					return
				})
				switch err {
				case nil:
					c.statusLock.Lock()
					if c.status == ConsumerDisconnected {
						c.consumer = consumer
						c.status = ConsumerConnected
					}
					c.statusLock.Unlock()
					break breakLoop
				case breaker.ErrBreakerOpen:
					log.Printf("kafka consumer connection failed, broker is open\n")
					if c.status == ConsumerDisconnected {
						time.AfterFunc(time.Second*5, func() {
							c.reConnect <- true
						})
					}
					break breakLoop
				default:
					log.Printf("kafka consumer connection error:%v\n", err)
				}
			}
		}
	}
}

// close 关闭连接
func (c *KafkaConsumer) close() error {
	c.statusLock.Lock()
	defer c.statusLock.Unlock()
	c.done = true
	return c.consumer.Close()
}
