package nats

import (
	"fmt"
	"log"
	"strconv"
	"testing"
	"time"
)

const (
	LocalUrl     = "nats://localhost:4222"
	LocalSubject = "subject1"
	TestNewUrl   = "nats://129.226.101.5:4222"
	TestUrl      = "nats://39.101.140.145:6222"
	CeniUrl      = "nats://58.240.113.38:10023"

	slot = 32
)

func TestTrans(t *testing.T) {
	nc, err := IConnect(CeniUrl)
	defer nc.IClose()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var dataStr string
	for slotSize := 0; slotSize < slot; slotSize += 1 {
		dataStr += "a"
	}

	go func() {
		_, err := nc.ISubscribe(LocalSubject, func(m *Msg) {
			//timeEnd := time.Now().UnixNano() / 1e6 // ms
			//fmt.Printf("receive: %v, %v\n", string(m.Data), timeEnd)
			fmt.Printf("receive: %v\n", string(m.Data))
		})
		if err != nil {
			log.Fatalf("Unexpected error: %v", err)
		}
	}()

	time.Sleep(2 * time.Second)

	err = nc.IPublish(LocalSubject, []byte(dataStr))
	fmt.Printf("send: %v\n", dataStr)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	time.Sleep(2 * time.Second)
}

func TestSingleTransmission(t *testing.T) {
	for i := 204; i <= 256; i++ {
		SingleTransmission(t, i)
	}
}

func SingleTransmission(t *testing.T, size int) {
	nc, err := IConnect(LocalUrl)
	defer nc.IClose()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var dataStr string
	for slotSize := 0; slotSize < slot; slotSize += 1 {
		dataStr += "a"
	}

	var sendData string

	tch := make(chan int64)
	sizeCh := make(chan int)

	go func() {
		_, err = nc.ISubscribe(LocalSubject+strconv.Itoa(size), func(m *Msg) {
			timeEnd := time.Now().UnixNano() / 1e6 // ms
			if len(m.Data)%slot != 0 || size != len(m.Data)/slot {
				log.Fatalf("invalid %v message (should be %v): got %v\n",
					len(m.Data), size*slot, string(m.Data))
			}
			if m.Data != nil {
				//fmt.Printf("receivetime %v : %v\n", size, timeEnd)
				tch <- timeEnd
				sizeCh <- size
				return
			}
		})
		if err != nil {
			log.Fatalf("Subscribe Unexpected error: %v", err)
		}

	}()

	time.Sleep(time.Second)

	for i := 1; i <= size; i++ {
		sendData += dataStr
	}

	timeBegin := time.Now().UnixNano() / 1e6 // ms
	err = nc.IPublish(LocalSubject+strconv.Itoa(size), []byte(sendData))
	if err != nil {
		t.Fatalf(" Publish Unexpected error: %v", err)
	}

	timeEnd := <-tch
	endSize := <-sizeCh
	//fmt.Printf("receive time : %v, size: %v\n", timeEndTmp, endSize)
	//
	if endSize != size {
		t.Fatalf("size error: got %v, should be %v\n", endSize, size)
	}

	timeStamp := timeEnd - timeBegin
	fmt.Printf("%v:   %v       %v        %v\n", size, timeStamp, timeEnd, timeBegin)

	time.Sleep(time.Second * 2)
}

func TestTransmission(t *testing.T) {
	nc, err := IConnect(CeniUrl)
	defer nc.IClose()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var dataStr string
	size := 1
	for slotSize := 0; slotSize < slot; slotSize += 1 {
		dataStr += "a"
	}

	var sendData string

	tch := make(chan int64)
	sizeCh := make(chan int)

	go func() {
		_, err = nc.ISubscribe(LocalSubject+strconv.Itoa(size), func(m *Msg) {
			timeEnd := time.Now().UnixNano() / 1e3 // millis
			if len(m.Data)%slot != 0 || size != len(m.Data)/slot {
				log.Fatalf("invalid %v message (should be %v): got %v\n",
					len(m.Data), size*slot, string(m.Data))
			}
			if m.Data != nil {
				//fmt.Printf("receivetime %v : %v\n", size, timeEnd)
				tch <- timeEnd
				sizeCh <- size
				return
			}
		})
		if err != nil {
			log.Fatalf("Subscribe Unexpected error: %v", err)
		}

	}()

	time.Sleep(time.Second)

	for i := 1; i <= size; i++ {
		sendData += dataStr
	}

	for size = 1; size <= 2; size++ {
		for cycle := 0; cycle < 100; cycle++ {
			timeBegin := time.Now().UnixNano() / 1e3 // millis
			err = nc.IPublish(LocalSubject+strconv.Itoa(size), []byte(sendData))
			if err != nil {
				t.Fatalf(" Publish Unexpected error: %v", err)
			}
			timeEnd := <-tch
			endSize := <-sizeCh
			//fmt.Printf("receive time : %v, size: %v\n", timeEndTmp, endSize)
			//
			if endSize != size {
				t.Fatalf("size error: got %v, should be %v\n", endSize, size)
			}

			timeStamp := timeEnd - timeBegin
			fmt.Printf("%v:  %v     %v      %v\n", cycle, timeStamp, timeEnd, timeBegin)

			time.Sleep(time.Second)
		}
	}
}

func TestSlowConsumer(t *testing.T) {
	size := 380
	const bps = 1

	const ms = 1000 / bps

	nc, err := IConnect(TestNewUrl)
	defer nc.IClose()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	go func() {
		_, err = nc.ISubscribe(LocalSubject, func(m *Msg) {
			fmt.Printf("receive: %v\n", string(m.Data)[:10])
		})
		if err != nil {
			log.Fatalf("Subscribe Unexpected error: %v", err)
		}
	}()

	var dataStr string
	var sendData string
	for slotSize := 0; slotSize < slot; slotSize += 1 {
		dataStr += "a"
	}

	for i := 1; i <= size; i++ {
		sendData += dataStr
	}

	for i := 1; i < 300; i++ {
		for j := 1; j <= bps; j++ {
			err = nc.IPublish(LocalSubject, []byte(strconv.Itoa(i)+sendData))
			if err != nil {
				log.Fatalf(" Publish Unexpected error: %v", err)
			}
			time.Sleep(time.Millisecond * ms)
		}

	}
}
