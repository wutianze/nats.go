package nats_test

import (
	"fmt"
	"time"

	"github.com/wutianze/nats.go"
)

const SubjectName = "subject1"

func ExampleIConnect() {
	nc, _ := nats.IConnect("nats://152.136.134.100:4222")
	defer nc.IClose()
}

func ExampleConn_IClose() {
	nc, _ := nats.IConnect("nats://152.136.134.100:4222")
	defer nc.IClose()
}

func ExampleConn_ISubscribe() {
	nc, _ := nats.IConnect("nats://152.136.134.100:4222")
	defer nc.IClose()

	nc.ISubscribe(SubjectName, func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	})

	// Output:
	// Received a message: hello world
}

func ExampleConn_IPublish() {
	nc, _ := nats.IConnect("nats://152.136.134.100:4222")
	defer nc.IClose()

	nc.IPublish(SubjectName, []byte("hello world"))
}

func ExampleConn_IRequest() {
	nc, _ := nats.IConnect("nats://152.136.134.100:4222")
	defer nc.IClose()

	data, _ := nc.IRequest(SubjectName, []byte("reply\n"), 3*time.Second)
	fmt.Printf("reply received: %s\n", string(data))

	// Output:
	// reply received: received and reply!
}

func ExampleMsg_IRespond() {
	nc, _ := nats.IConnect("nats://152.136.134.100:4222")
	defer nc.IClose()

	nc.ISubscribe(SubjectName, func(m *nats.Msg) {
		if err := m.IRespond([]byte("received and reply! \n")); err != nil {
			return
		}
		fmt.Printf("request received: %v\n", string(m.Data))
	})

	// Output:
	// request received: reply
}

func ExampleConn_IFlush() {
	nc, _ := nats.IConnect("nats://152.136.134.100:4222")
	defer nc.IClose()

	msg := &nats.Msg{Subject: "foo", Reply: "bar", Data: []byte("Hello World!")}
	for i := 0; i < 1000; i++ {
		nc.PublishMsg(msg)
	}

	nc.IFlush()
}

func ExampleSubscription_IUnsubscribe() {
	nc, _ := nats.IConnect("nats://152.136.134.100:4222")
	defer nc.IClose()

	sub, _ := nc.ISubscribe(SubjectName, func(m *nats.Msg) {})

	sub.IUnsubscribe()
}

// the function below belongs to Nats JetStream that provides stream transmission,
// load balance and QoS

// the function below belongs to Nats JetStream that provides stream transmission,
// load balance and QoS
