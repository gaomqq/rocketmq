package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

func main() {
	//创建 RocketMQ 生产者
	p, err := rocketmq.NewProducer(
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"111.229.76.218:9876"})),
		producer.WithRetry(2), //指定重试次数
	)

	if err != nil {
		panic(err)
	}
	//启动生产者
	if err = p.Start(); err != nil {
		panic("启动producer失败")
	}
	topic := "testTopic"
	// 构建一个消息
	message := primitive.NewMessage(topic, []byte("苏杭2号"))
	// 给message设置延迟级别
	message.WithDelayTimeLevel(5)
	//发送消息
	res, err := p.SendSync(context.Background(), message)
	if err != nil {
		fmt.Printf("消息发送失败: %s\n", err)
	} else {
		fmt.Printf("消息发送成功=%s\n", res.String())
	}
	//关闭生产者
	if err = p.Shutdown(); err != nil {
		panic("关闭producer失败")
	}
}
