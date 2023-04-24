### go-kafka golang使用kafka
___


##### 1、环境开启

```
cd  go-kafka/deployments && docker-compose up -d
```

##### 2、生产者

```
cd go-kafka/tests/producer && go run main.go

kafka syncProducer connection success producer-name:default-sync-producer
sync send message success partition:0, offset:7
```


##### 3、消费者

```
cd go-kafka/tests/consumer && go run main.go

consumer receive message: topic:hqd8080, partition:0, offset:7, value:{"id":1,"name":"test!!","create_at":"2023-04-24T17:20:06+08:00"}
```