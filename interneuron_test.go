package nats

import (
	"errors"
	"fmt"
	"log"
	"testing"
	"time"
)

const (
	ServerUrl       = "nats://152.136.134.100:4222"
	NKUrl           = "nats://152.136.134.100:4223"
	AliyunUrl       = "nats://39.101.140.145:4222"
	SubjectName     = "subject1"
	StreamName      = "stream1"
	TestStreamName  = "foo"
	TestSubjectName = "foo"
)

type Request struct {
	Data []byte
}

type Response struct {
	Data []byte
}

type RpcFunc func(request Request, response *Response) int

func test1(request Request, response *Response) int {
	fmt.Println("1")
	return 3
}

func test2(request Request, response *Response) int {
	fmt.Println("2")
	return 4
}

func TestJsRPC(t *testing.T) {
	nc, js := JetStreamInit(t, `nats://152.136.134.100:4222`)
	defer nc.IClose()

	js.IDeleteStream("test-1")
	js.IDeleteStream("test-2")
	js.IAddStream("test-1", "test-1")
	js.IAddStream("test-2", "test-2")

	funcMap := make(map[string]RpcFunc)

	//funcMap["test-2"] = test2
	funcMap["test-1"] = test1

	for pt, fc := range funcMap {
		fmt.Printf("pt:%v fc:%v\n", pt, fc)
		f := func(msg *Msg) {
			fmt.Printf("inter f:%v\n", fc)
			num := fc(Request{}, &Response{})
			fmt.Printf("ret num %v\n", num)
		}

		fmt.Printf("f:%v\n", &f)

		js.ISubscribe(pt, f)
	}

	//js.IRequest("test-2", nil, 5*time.Second)
	js.IRequest("test-1", nil, 5*time.Second)

}

func TestJsRawReqResp(t *testing.T) {
	// 开启了js的server会导致request的返回值发生变化：由本应该的字符串变成“{"stream":"stream1", "seq":1}”形式的
	// json。这个问题应该是由于server-js使用一个默认的stream1来传输nats-core，导致request请求到的事实上是这个stream
	// 的信息。

	// request原理：把第一条（也可能是所有）传过来的msg消息放入channel，传给msg
	// 所以问题在于：response传过来的消息变成了json记录

	// js的pub其实就是req，只不过只提取了返回的ack

	nc, js := JetStreamInit(t, `nats://152.136.134.100:4222`)
	defer nc.IClose()
	subject2 := "test-2"

	var err error
	//err = js.DeleteStream(subject2)
	//if err != nil {
	//	log.Fatalf("Unexpected error: %v", err)
	//}

	_, err = js.IAddStream(subject2, subject2)
	if err != nil {
		log.Fatalf("IAddStream Unexpected error: %v", err)
	}

	_, err = js.ISubscribe(subject2, func(m *Msg) {
		if err = m.IRespond([]byte("received and reply! \n")); err != nil {
			log.Fatalf("ISubscribe Unexpected error: %v", err)
		}
		fmt.Printf("request received: %v\n", string(m.Data))
	})
	if err != nil {
		log.Fatalf("ISubscribe failed: %s", err)
		return
	}

	time.Sleep(time.Second)

	data, err := js.IRequest(subject2, []byte("requesting...\n"), 5*time.Second)
	if err != nil {
		log.Fatalf("IRequest Unexpected error: %v", err)
	}

	time.Sleep(time.Second)

	fmt.Printf("reply received: %s\n", string(data))
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
	nc, err := IConnect(AliyunUrl)
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
	nc, err := IConnect(AliyunUrl)
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
	nc, _ := IConnect(NKUrl)
	defer nc.IClose()

	data, err := nc.IRequest(SubjectName, []byte("reply\n"), 5*time.Second)
	if err != nil {
		log.Fatalf("IRequest failed: %v", err)
		return
	}
	fmt.Printf("reply received: %s\n", string(data))
}

func TestResp(t *testing.T) {
	nc, _ := IConnect(NKUrl)
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

func TestJSPubSub(t *testing.T) {
	nc, js := JetStreamInit(t, NKUrl)
	js.IDeleteStream(StreamName)
	js.IDeleteStream("foo")
	//go TestJsSubscribe(t)
	time.Sleep(time.Second * 3)
	TestJSPublish(t)
	TestJsSubscribe(t)

	//if err := js.IPurgeStream(StreamName); err != nil {
	//	t.Fatalf("Unexpected error: %v", err)
	//}

	defer nc.IClose()
}

func TestJSPublish(t *testing.T) {
	nc, js := JetStreamInit(t, NKUrl)
	defer nc.IClose()

	// Delete previous stream
	// js.IDeleteStream(StreamName)

	_, err := js.IAddStream(StreamName, SubjectName)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = js.IAddStream("foo", "foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	js.IPublish("foo", "foo")

	msg := "Test publish " + time.Now().Format("2006-01-02 15:04:05")
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

func TestJsSubscribe(t *testing.T) {
	nc, js := JetStreamInit(t, NKUrl)
	defer nc.IClose()

	_, err := js.IAddStream(StreamName, SubjectName)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

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
	_, err = js.ISubscribe("foo", func(m *Msg) {
		fmt.Printf("Received another message: %s\n", string(m.Data))
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	time.Sleep(time.Second * 2)
	defer sub.Unsubscribe()
}

func TestJSPublishLoadBalance(t *testing.T) {
	nc, js := JetStreamInit(t, ServerUrl)
	defer nc.IClose()

	// Delete previous stream
	// js.IDeleteStream(StreamName)

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
	c, err := InitNeuron(NKUrl)
	defer c.CloseNeuron()

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	pubStruct := new(struct {
		A int
		B bool
		C string
	})
	pubStruct.C = time.Now().Format("2006-01-02 15:04:05")

	err = c.Pub(pubStruct, &PubSubConfig{
		Topic:          "guid1",
		Mode:           Broadcast,
		DeletePrevious: true,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	time.Sleep(time.Second * 2)

	var msg struct {
		A int
		B bool
		C string
	}
	err = c.Sub(&PubSubConfig{
		Topic:     "guid1",
		Integrity: ExactlyOnce,
	}, &msg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fmt.Printf("received \"%v\"\n", msg)
}

func TestJsReqResp(t *testing.T) {
	var err error
	nc, js := JetStreamInit(t, NKUrl)
	defer nc.IClose()
	//
	//err = js.IDeleteStream(TestStreamName)
	//if err != nil {
	//	t.Fatalf("Unexpected error: %v", err)
	//}
	_, err = js.IAddStream(TestStreamName, TestSubjectName)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	go TestJsRequest(t)
	time.Sleep(time.Second)
	TestJsResponse(t)
	time.Sleep(time.Second * 2)
}

func TestJsRequest(t *testing.T) {
	var err error
	nc, js := JetStreamInit(t, NKUrl)
	defer nc.IClose()

	respData, err := js.IRequest(TestSubjectName, []byte("data_foo"), time.Second*5)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fmt.Printf("client 1 received: \"%v\"\n", string(respData))

	err = js.IDeleteStream(TestStreamName)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJsResponse(t *testing.T) {
	nc, js := JetStreamInit(t, NKUrl)
	defer nc.IClose()

	reqData, err := js.IResponse(TestSubjectName, []byte("receiver received!"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fmt.Printf("client 2 received: \"%v\"\n", string(reqData))
}
