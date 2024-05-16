package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"os"
	"time"
)

type DemoListener struct {
}

func (dl *DemoListener) ExecuteLocalTransaction(msg *primitive.Message) primitive.LocalTransactionState {
	fmt.Println("开始执行本地逻辑")
	// 这里模拟本地事务执行，如果成功，则返回 CommitMessageState，否则返回 RollbackMessageState
	// 你需要根据实际业务逻辑来实现本地事务处理
	// 如果本地事务执行成功，则将消息标记为可提交状态
	// 如果本地事务执行失败，则将消息标记为回滚状态
	// 这里简单起见，直接返回 CommitMessageState

	//return primitive.RollbackMessageState
	return primitive.CommitMessageState
}

func (dl *DemoListener) CheckLocalTransaction(msg *primitive.MessageExt) primitive.LocalTransactionState {
	fmt.Println("rocketmq的消息回查")
	// 这里模拟消息回查，如果本地事务成功，则返回 CommitMessageState，否则返回 RollbackMessageState
	// 这里简单起见，直接返回 CommitMessageState

	return primitive.CommitMessageState
}

func main() {
	var p, _ = rocketmq.NewTransactionProducer(
		&DemoListener{},
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"111.229.76.218:9876"})),
		producer.WithRetry(2),
	)
	err := p.Start()
	if err != nil {
		fmt.Printf("生产者启动失败: %s\n", err.Error())
		os.Exit(1)
	}

	res, err := p.SendMessageInTransaction(context.Background(),
		primitive.NewMessage("testTopic", []byte("苏杭3号")))

	if err != nil {
		fmt.Printf("消息发送失败: %s\n", err)
	} else {
		fmt.Printf("消息发送成功=%s\n", res.String())
	}

	time.Sleep(5 * time.Minute)
	err = p.Shutdown()
	if err != nil {
		fmt.Printf("shutdown producer error: %s", err.Error())
	}
}
