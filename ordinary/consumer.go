package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"time"
)

func main() {

	//创建RocketMQ的消费者实例
	c, err := rocketmq.NewPushConsumer(
		//指定了消费者组名称为 "test"
		consumer.WithGroupName("test"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"111.229.76.218:9876"})),
	)

	if err != nil {
		panic(err)
	}

	//订阅主题 "testTopic"
	if err := c.Subscribe("testTopic",
		consumer.MessageSelector{},
		// 收到消息后的回调函数
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for i := range msgs {
				fmt.Printf("获取到值： %v \n", msgs[i])
			}
			return consumer.ConsumeSuccess, nil
		}); err != nil {
	}
	//启动消费者
	err = c.Start()
	if err != nil {
		panic("启动consumer失败")
	}
	// 不能让主goroutine退出
	time.Sleep(time.Hour)
	//在程序结束时关闭消费者，释放资源
	_ = c.Shutdown()
}
