package mq

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Shopify/sarama"
	"github.com/eapache/go-resiliency/breaker"
	"github.com/pkg/errors"
)

const (
	ProducerConnected    string = "connected"
	ProducerDisconnected string = "disconnected"
	ProducerClosed       string = "closed"
	DefaultAsyncProduct  string = "default-async-producer"
	DefaultSyncProducer  string = "default-sync-producer"
)

// KafkaProducer kafka生产者信息
type KafkaProducer struct {
	Name       string
	Hosts      []string
	Config     *sarama.Config
	Status     string
	Breaker    *breaker.Breaker
	ReConnect  chan bool
	StatusLock sync.Mutex
}

// SyncProducer 同步生产者
type SyncProducer struct {
	KafkaProducer
	SyncProducer *sarama.SyncProducer
}

// AsyncProducer 异步生产者
type AsyncProducer struct {
	KafkaProducer
	AsyncProducer *sarama.AsyncProducer
}

var (
	ErrProducerTimeout  = errors.New("push message timeout")
	kafkaSyncProducers  = make(map[string]*SyncProducer)
	kafkaAsyncProducers = make(map[string]*AsyncProducer)
)

type Message struct {
	Topic     string
	KeyBytes  []byte
	DataBytes []byte
}

func MessageValueByteEncoder(value []byte) sarama.Encoder {
	return sarama.ByteEncoder(value)
}

func MessageValueStrEncoder(value string) sarama.Encoder {
	return sarama.StringEncoder(value)
}

// GetDefaultProductConfig kafka生产者默认配置
func GetDefaultProductConfig(clientID string) (config *sarama.Config) {
	config = sarama.NewConfig()
	config.ClientID = clientID
	config.Version = sarama.V2_0_0_0
	config.Net.DialTimeout = time.Second * 30
	config.Net.ReadTimeout = time.Second * 30
	config.Net.WriteTimeout = time.Second * 30

	config.Producer.Retry.Backoff = time.Millisecond * 500
	config.Producer.Retry.Max = 3
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.MaxMessageBytes = 1000000 * 2
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Partitioner = sarama.NewHashPartitioner
	config.Producer.Compression = sarama.CompressionLZ4
	return
}

// InitSyncProducer 初始化同步模式生产者
func InitSyncProducer(name string, hosts []string, config *sarama.Config) error {
	syncProducer := &SyncProducer{}
	syncProducer.Name = name
	syncProducer.Hosts = hosts
	syncProducer.Status = ProducerDisconnected
	if config == nil {
		config = GetDefaultProductConfig(name)
	}
	syncProducer.Config = config
	if producer, err := sarama.NewSyncProducer(hosts, config); err != nil {
		return errors.Wrap(err, "kafka newSyncProducer err name:"+name)
	} else {
		syncProducer.Breaker = breaker.New(3, 1, time.Second*2)
		syncProducer.ReConnect = make(chan bool)
		syncProducer.SyncProducer = &producer
		syncProducer.Status = ProducerConnected
		log.Printf("kafka syncProducer connection success producer-name:%s\n", name)
	}
	go syncProducer.keepConnect()
	go syncProducer.check()
	kafkaSyncProducers[name] = syncProducer
	return nil
}

// InitAsyncProducer 初始化异步模式生产者
func InitAsyncProducer(name string, hosts []string, config *sarama.Config) error {
	asyncProducer := &AsyncProducer{}
	asyncProducer.Name = name
	asyncProducer.Hosts = hosts
	asyncProducer.Status = ProducerDisconnected
	if config == nil {
		config = GetDefaultProductConfig(name)
	}
	asyncProducer.Config = config
	if producer, err := sarama.NewAsyncProducer(hosts, config); err != nil {
		return errors.Wrap(err, "kafka newAsyncProducer err name:"+name)
	} else {
		asyncProducer.Breaker = breaker.New(3, 1, time.Second*5)
		asyncProducer.ReConnect = make(chan bool)
		asyncProducer.AsyncProducer = &producer
		asyncProducer.Status = ProducerConnected
		log.Printf("kafka asyncProducer connection success name:%s\n", name)
	}
	go asyncProducer.keepConnect()
	go asyncProducer.check()
	kafkaAsyncProducers[name] = asyncProducer
	return nil
}

// SendMessages 同步发送多条消息
func (syncProducer *SyncProducer) SendMessages(msg []*sarama.ProducerMessage) (errs sarama.ProducerErrors) {
	if syncProducer.Status != ProducerConnected {
		errs = append(errs, &sarama.ProducerError{
			Err: errors.New("kafka syncProducer:" + syncProducer.Status),
		})
		return
	}
	errs = (*syncProducer.SyncProducer).SendMessages(msg).(sarama.ProducerErrors)
	for _, err := range errs {
		if errors.Is(err, sarama.ErrBrokerNotAvailable) {
			syncProducer.StatusLock.Lock()
			if syncProducer.Status == ProducerConnected {
				syncProducer.Status = ProducerDisconnected
				syncProducer.ReConnect <- true
			}
			syncProducer.StatusLock.Unlock()
		}
	}
	return
}

// SendMessage 同步发送单条消息
func (syncProducer *SyncProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	if syncProducer.Status != ProducerConnected {
		return -1, -1, errors.New("kafka syncProducer " + syncProducer.Status)
	}
	partition, offset, err = (*syncProducer.SyncProducer).SendMessage(msg)
	if errors.Is(err, sarama.ErrBrokerNotAvailable) {
		syncProducer.StatusLock.Lock()
		if syncProducer.Status == ProducerConnected {
			syncProducer.Status = ProducerDisconnected
			syncProducer.ReConnect <- true
		}
		syncProducer.StatusLock.Unlock()
	}
	return
}

// check 同步生产者状态检查
func (syncProducer *SyncProducer) check() {
	defer func() {
		log.Printf("kafka syncProducer check exited\n")
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		if syncProducer.Status == ProducerClosed {
			return
		}
		select {
		case <-signals:
			syncProducer.StatusLock.Lock()
			syncProducer.Status = ProducerClosed
			syncProducer.StatusLock.Unlock()
			return
		}
	}
}

// keepConnect 同步模式保持断线重连
func (syncProducer *SyncProducer) keepConnect() {
	defer func() {
		log.Printf("kafka syncProducer keepConnect exited\n")
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		if syncProducer.Status == ProducerClosed {
			return
		}
		select {
		case <-signals:
			syncProducer.StatusLock.Lock()
			syncProducer.Status = ProducerClosed
			syncProducer.StatusLock.Unlock()
			return
		case <-syncProducer.ReConnect:
			if syncProducer.Status != ProducerDisconnected {
				break
			}
			log.Printf("kafka syncProducer ReConnecting... name:%s\n", syncProducer.Name)
			var producer sarama.SyncProducer
		syncBreakLoop:
			for {
				err := syncProducer.Breaker.Run(func() (err error) {
					sarama.NewSyncProducer(syncProducer.Hosts, syncProducer.Config)
					return
				})
				switch err {
				case nil:
					syncProducer.StatusLock.Lock()
					if syncProducer.Status == ProducerDisconnected {
						syncProducer.SyncProducer = &producer
						syncProducer.Status = ProducerConnected
					}
					syncProducer.StatusLock.Unlock()
					log.Printf("kafka syncProducer ReConnected, name:%s\n", syncProducer.Name)
					break syncBreakLoop
				case breaker.ErrBreakerOpen:
					log.Printf("kafka connection fail, broker is open\n")
					if syncProducer.Status == ProducerDisconnected {
						time.AfterFunc(time.Second*2, func() {
							log.Printf("kafka begin to ReConnection of ErrBreakerOpen\n")
							syncProducer.ReConnect <- true
						})
					}
					break syncBreakLoop
				default:
					log.Printf("kafka Reconnection err:%v, name:%s\n", err, syncProducer.Name)
				}
			}
		}
	}
}

// Close 关闭生产者
func (syncProducer *SyncProducer) Close() error {
	syncProducer.StatusLock.Lock()
	defer syncProducer.StatusLock.Unlock()
	err := (*syncProducer.SyncProducer).Close()
	syncProducer.Status = ProducerClosed
	return err
}

// AsyncSendMessage 异步模式发送信息
func (asyncProducer *AsyncProducer) AsyncSendMessage(msg *sarama.ProducerMessage) error {
	var err error
	if asyncProducer.Status != ProducerConnected {
		return errors.New("kafka disconnected")
	}
	(*asyncProducer.AsyncProducer).Input() <- msg
	return err
}

// check 异步模式生产者状态检查
func (asyncProducer *AsyncProducer) check() {
	defer func() {
		log.Printf("kafka asyncProducer check exited\n")
	}()
	for {
		switch asyncProducer.Status {
		case ProducerDisconnected:
			time.Sleep(time.Second * 5)
			continue
		case ProducerClosed:
			return
		}
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
		for {
			select {
			case msg := <-(*asyncProducer.AsyncProducer).Successes():
				log.Printf("kafka produce message success:[%s]-%q\n", msg.Topic, msg.Value)
			case err := <-(*asyncProducer.AsyncProducer).Errors():
				log.Printf("kafka send message err:%v\n", err)
				if errors.Is(err, sarama.ErrOutOfBrokers) || errors.Is(err, sarama.ErrNotConnected) {
					asyncProducer.StatusLock.Lock()
					if asyncProducer.Status == ProducerConnected {
						asyncProducer.Status = ProducerDisconnected
						asyncProducer.ReConnect <- true
					}
					asyncProducer.StatusLock.Unlock()
				}
			case s := <-signals:
				log.Printf("kafka asyncProducer receive system signal:%s, name:%s\n", s.String(), asyncProducer.Name)
				asyncProducer.Status = ProducerClosed
				return
			}
		}
	}
}

// keepConnect 异步模式保持断线重连
func (asyncProducer *AsyncProducer) keepConnect() {
	defer func() {
		log.Printf("asyncProducer keepConnect exited\n")
	}()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	for {
		if asyncProducer.Status == ProducerClosed {
			return
		}
		select {
		case s := <-signals:
			log.Printf("kafka asyncProducer receive system signal:%s, name:%s\n", s.String(), asyncProducer.Name)
			asyncProducer.Status = ProducerClosed
			return
		case <-asyncProducer.ReConnect:
			if asyncProducer.Status != ProducerDisconnected {
				break
			}
			log.Printf("kafka asyncProducer Reconnecting... name:%s\n", asyncProducer.Name)
			var producer sarama.AsyncProducer
		asyncBreakLoop:
			for {
				err := asyncProducer.Breaker.Run(func() (err error) {
					producer, err = sarama.NewAsyncProducer(asyncProducer.Hosts, asyncProducer.Config)
					return
				})
				switch err {
				case nil:
					asyncProducer.StatusLock.Lock()
					if asyncProducer.Status == ProducerDisconnected {
						asyncProducer.AsyncProducer = &producer
						asyncProducer.Status = ProducerConnected
					}
					asyncProducer.StatusLock.Unlock()
					log.Printf("kafka asyncProducer ReConnection success, name:%s\n", asyncProducer.Name)
					break asyncBreakLoop
				case breaker.ErrBreakerOpen:
					log.Printf("kafka connection fail, broker is open\n")
					if asyncProducer.Status == ProducerDisconnected {
						time.AfterFunc(time.Second*2, func() {
							log.Printf("kafka begin to ReConnect, because of ErrBreakerOpen\n")
							asyncProducer.ReConnect <- true
						})
					}
					break asyncBreakLoop
				default:
					log.Printf("kafka ReConnect error:%v, name:%s\n", err, asyncProducer.Name)
				}
			}
		}
	}
}

// Close 关闭生产者
func (asyncProducer *AsyncProducer) Close() error {
	asyncProducer.StatusLock.Lock()
	defer asyncProducer.StatusLock.Unlock()
	err := (*asyncProducer.AsyncProducer).Close()
	asyncProducer.Status = ProducerClosed
	return err
}

// GetSyncProducer 获取同步模式生产者对象
func GetSyncProducer(name string) *SyncProducer {
	if producer, ok := kafkaSyncProducers[name]; ok {
		return producer
	} else {
		log.Printf("kafka InitSyncProducer must be called\n")
		return nil
	}
}

// GetAsyncProducer 获取异步模式生产者对象
func GetAsyncProducer(name string) *AsyncProducer {
	if producer, ok := kafkaAsyncProducers[name]; ok {
		return producer
	} else {
		log.Printf("kafka InitAsyncProducer must be called\n")
		return nil
	}
}
