package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

func main() {
	//实例化 -- 生产者发送消息
	client, err := rocketmq.NewProducer(producer.WithNameServer([]string{"111.229.76.218:9876"}))
	if err != nil {
		panic("Error connection to server")
	}
	//启动生产者
	if err := client.Start(); err != nil {
		panic("生产者启动失败")
	}
	//发送消息
	result, err := client.SendSync(context.Background(), primitive.NewMessage("testTopic", []byte("苏杭1号")))
	if err != nil {
		panic(err)
	}
	fmt.Println(result)
	//关闭生产者
	if err := client.Shutdown(); err != nil {
		panic("Error Shutdown producer")
	}
}
