package nats

import (
	"fmt"
	"log"
	"testing"
	"time"
)

const ServerUrl = "nats://152.136.134.100:4222"
const SubjectName = "subject1"

func TestRPC(t *testing.T) {
	// Client pub-sub
	TestPubSub(t)
	// Client req-res

}

func TestPubSub(t *testing.T) {
	go TestSub(t)
	time.Sleep(time.Second)
	TestPub(t)
	time.Sleep(time.Second * 2)
}

func TestReqResp(t *testing.T) {
	go TestResp(t)
	time.Sleep(time.Second)
	TestReq(t)
	time.Sleep(time.Second * 2)
}

func TestPub(t *testing.T) {
	nc, err := IConnect(ServerUrl)
	if err != nil {
		log.Fatalf("IConnect failed: %v", err)
		return
	}
	defer nc.IClose()

	if err = nc.IPublish(SubjectName, []byte("hello world")); err != nil {
		log.Fatalf("IPublish failed: %v", err)
		return
	}

	fmt.Printf("TestPub finished\n")
}

func TestSub(t *testing.T) {
	nc, err := IConnect(ServerUrl)
	if err != nil {
		log.Fatalf("IConnect failed: %v", err)
		return
	}

	defer nc.IClose()

	for {
		_, err = nc.ISubscribe(SubjectName, func(m *Msg) {
			fmt.Printf("Received a message: %s\n", string(m.Data))
		})
		if err != nil {
			log.Fatalf("ISubscribe failed: %v", err)
			return
		}
	}
}

func TestReq(t *testing.T) {
	nc, _ := IConnect(ServerUrl)
	defer nc.IClose()

	data, err := nc.IRequest(SubjectName, []byte("reply\n"), 5*time.Second)
	if err != nil {
		log.Fatalf("IRequest failed: %v", err)
		return
	}
	fmt.Printf("reply received: %s\n", string(data))
}

func TestResp(t *testing.T) {
	nc, _ := IConnect(ServerUrl)
	defer nc.IClose()

	// Subscribe
	for {
		_, err := nc.ISubscribe(SubjectName, func(m *Msg) {
			if err := m.IRespond([]byte("received and reply! \n")); err != nil {
				return
			}
			fmt.Printf("request received: %v\n", string(m.Data))
		})
		if err != nil {
			log.Fatalf("ISubscribe failed: %s", err)
			return
		}
	}
}

func TestFlush(t *testing.T) {
	nc, _ := IConnect(ServerUrl)
	defer nc.IClose()

	msg := &Msg{Subject: "foo", Reply: "bar", Data: []byte("Hello World!")}
	for i := 0; i < 1000; i++ {
		if err := nc.PublishMsg(msg); err != nil {
			log.Fatalf("PublishMsg failed: %s", err)
			return
		}
	}
	err := nc.IFlush()
	if err != nil {
		log.Fatalf("IFlush failed: %s", err)
		return
	}
}
