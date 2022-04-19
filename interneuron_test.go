package nats

import (
	"errors"
	"fmt"
	"log"
	"testing"
	"time"
)

const ServerUrl = "nats://152.136.134.100:4222"
const SubjectName = "subject1"
const StreamName = "stream1"

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

// JetStream Usage Test

func JetStreamInit(t *testing.T, url string) (*Conn, JetStreamContext) {
	nc, err := IConnect(url)
	if err != nil {
		t.Fatalf("Unexpected error connecting: %v", err)
	}
	js, err := nc.IJetStream(MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}

	return nc, js
}

func TestJSPublish(t *testing.T) {
	nc, js := JetStreamInit(t, ServerUrl)
	defer nc.IClose()

	// Delete previous stream
	// js.IDeleteStream(StreamName)

	_, err := js.IAddStream(StreamName, SubjectName)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	msg := []byte("Test publish" + time.Now().Format("2006-01-02 15:04:05"))
	pa, err := js.IPublish(SubjectName, msg)
	if err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	if err != nil {
		t.Fatalf("stream lookup failed: %v", err)
	}
	if pa == nil || pa.Sequence != 1 || pa.Stream != StreamName {
		t.Fatalf("Wrong stream sequence, expected 1, got %d", pa.Sequence)
	}
	stream, err := js.StreamInfo(StreamName)
	if stream.State.Msgs != 1 {
		t.Fatalf("Expected 1 messages, got %d", stream.State.Msgs)
	}
}

func TestJSPublishLoadBalance(t *testing.T) {
	nc, js := JetStreamInit(t, ServerUrl)
	defer nc.IClose()

	// Delete previous stream
	js.IDeleteStream(StreamName)

	_, err := js.IAddStreamOneConsumer(StreamName, SubjectName)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	msg := []byte("Test publish")
	pa, err := js.IPublish(SubjectName, msg)
	if err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	if pa == nil || pa.Sequence != 1 || pa.Stream != StreamName {
		t.Fatalf("Wrong stream sequence, expected 1, got %d", pa.Sequence)
	}
	stream, err := js.StreamInfo(StreamName)
	if stream.State.Msgs != 1 {
		t.Fatalf("Expected 1 messages, got %d", stream.State.Msgs)
	}
	if err != nil {
		t.Fatalf("stream lookup failed: %v", err)
	}
}

func TestJsSubscribe(t *testing.T) {
	nc, js := JetStreamInit(t, ServerUrl)
	defer nc.IClose()

	stream, err := js.StreamInfo(StreamName)
	if stream.State.Msgs != 1 {
		// t.Fatalf("Expected 1 messages, got %d", stream.State.Msgs)
	}

	sub, err := js.ISubscribe(SubjectName, func(m *Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	time.Sleep(time.Second * 2)
	defer sub.Unsubscribe()
}

func TestJSSubscribeLoadBalance(t *testing.T) {
	nc, js := JetStreamInit(t, ServerUrl)
	defer nc.IClose()

	stream, err := js.StreamInfo(StreamName)
	if stream.State.Msgs != 1 {
		// t.Fatalf("Expected 1 messages, got %d", stream.State.Msgs)
	}

	sub, _ := js.ISubscribe(SubjectName, func(m *Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	})

	sub, err = js.ISubscribe(SubjectName, func(m *Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	})
	if err != errors.New("nats: maximum consumers limit reached") {
		fmt.Printf("error maximum consumers limit reached occurs: %v\n", err)
	}

	time.Sleep(time.Second * 2)
	defer sub.Unsubscribe()
}

func TestJsSubscribeLastMsg(t *testing.T) {
	nc, js := JetStreamInit(t, ServerUrl)
	defer nc.IClose()

	stream, err := js.StreamInfo(StreamName)
	if stream.State.Msgs != 1 {
		// t.Fatalf("Expected 1 messages, got %d", stream.State.Msgs)
	}

	sub, err := js.ISubscribeLastMsg(SubjectName, func(m *Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	time.Sleep(time.Second * 2)
	defer sub.Unsubscribe()
}

func TestPubSub_Packed(t *testing.T) {
	c, err := InitNeuron("nats://152.136.134.100:4222")
	defer c.CloseNeuron()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	/*
		c.js.DeleteStream("guid1")
		_, err = c.js.AddStream(&StreamConfig{
			Name:         "guid1",
			Subjects:     []string{"guid1"},
			MaxConsumers: 1,
			Replicas:     1,
		})
		_, err = c.js.AddStream(&StreamConfig{
			Name:         "guid1",
			Subjects:     []string{"guid1"},
			MaxConsumers: 1,
			Replicas:     1,
		})
		if err != nil {
			t.Fatalf("error: %v", err)
			return
		}
	*/
	pubMsg := time.Now().Format("2006-01-02 15:04:05")
	err = c.Pub(pubMsg, &PubSubConfig{
		Interneuron:    "guid1",
		Mode:           Broadcast,
		DeletePrevious: true,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	time.Sleep(time.Second * 4)

	msg, err := c.Sub(&PubSubConfig{
		Interneuron: "guid1",
		Integrity:   ExactlyOnce,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fmt.Printf("received \"%v\"\n", msg)
}
