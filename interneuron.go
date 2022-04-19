package nats

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"
)

type Controller struct {
	nc   *Conn
	js   JetStreamContext
	jsId string
}

const (
	// StreamID = "interneuron_stream_id"

	Broadcast  = "broadcast"
	PeerToPeer = "peer-to-peer"

	ExactlyOnce = "exactly-once"
	AtLeastOnce = "at-least-once"
)

type PubSubConfig struct {
	Interneuron string `json:"interneuron"`

	Mode      string `json:"mode"`
	Integrity string `json:"integrity"`
	Latency   string `json:"latency"`

	DeletePrevious bool `json:"delete-previous"`
}

type IJetStream interface {
	IStreamInfo(stream string) (*StreamInfo, error)

	IAddStream(stream string, subj string) (*StreamInfo, error)

	IAddStreamOneConsumer(stream string, subj string) (*StreamInfo, error)

	IPurgeStream(stream string) error

	IDeleteStream(name string) error

	IPublish(subj string, v interface{}) (*PubAck, error)

	IPublishMsg(m *Msg) (*PubAck, error)

	ISubscribe(subj string, cb MsgHandler) (*Subscription, error)

	ISubscribeLastMsg(subj string, cb MsgHandler) (*Subscription, error)
}

// IJetStream creates a JetStreamContext for messaging and stream management.
// The context will be used for controlling the stream transmission
// opts could be wait time
func (nc *Conn) IJetStream(opts ...JSOpt) (JetStreamContext, error) {
	return nc.JetStream(opts...)
}

// IStreamInfo provides stream information
// return stream info
func (js *js) IStreamInfo(stream string) (*StreamInfo, error) {
	return js.StreamInfo(stream)
}

// IAddStream adds one stream into JetStream permanently
// Once a stream is added with stream name and subject name, it is needless to
// call this function again while using new JetStreamContext.
func (js *js) IAddStream(stream string, subj string) (*StreamInfo, error) {
	return js.AddStream(&StreamConfig{
		Name:     stream,
		Subjects: []string{subj},
	})
}

// IAddStreamOneConsumer adds one stream as IAddStream does, simply this function
// creates stream that can only be subscribed by ONE consumer
func (js *js) IAddStreamOneConsumer(stream string, subj string) (*StreamInfo, error) {
	return js.AddStream(&StreamConfig{
		Name:         stream,
		Subjects:     []string{subj},
		MaxConsumers: 1,
	})
}

// IPurgeStream purges all messages from one stream
func (js *js) IPurgeStream(stream string) error {
	return js.purgeStream(stream, nil)
}

// IDeleteStream deletes one stream
func (js *js) IDeleteStream(name string) error {
	return js.DeleteStream(name)
}

// IPublish publish a message to the stream asynchronously
func (js *js) IPublish(subj string, v interface{}) (*PubAck, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return js.IPublishMsg(&Msg{Subject: subj, Data: data})
}

func (js *js) IPublishMsg(m *Msg) (*PubAck, error) {
	return js.PublishMsg(m)
}

// ISubscribe subscribes messages from a subject
func (js *js) ISubscribe(subj string, cb MsgHandler) (*Subscription, error) {
	if cb == nil {
		return nil, errors.New("nats: Handler required for EncodedConn Subscription")
	}
	natsCB := func(m *Msg) {
		cbValue := reflect.ValueOf(cb)
		argType, _ := argInfo(cb)
		var oPtr reflect.Value
		var oV []reflect.Value
		if argType.Kind() != reflect.Ptr {
			oPtr = reflect.New(argType)
		} else {
			oPtr = reflect.New(argType.Elem())
		}
		switch arg := oPtr.Interface().(type) {
		case *string:
			str := string(m.Data)
			if strings.HasPrefix(str, `"`) && strings.HasSuffix(str, `"`) {
				*arg = str[1 : len(str)-1]
			} else {
				*arg = str
			}
		case *[]byte:
			*arg = m.Data
		default:
			err := json.Unmarshal(m.Data, arg)
			if err != nil {
				return
			}
		}
		if argType.Kind() != reflect.Ptr {
			oPtr = reflect.Indirect(oPtr)
		}

		oV = []reflect.Value{oPtr}

		cbValue.Call(oV)
	}

	return js.Subscribe(subj, natsCB)
}

// ISubscribeLastMsg subscribes exactly the last message from a subject
func (js *js) ISubscribeLastMsg(subj string, cb MsgHandler) (*Subscription, error) {
	return js.Subscribe(subj, cb, DeliverLast())
}

// The interneuron.go needs to provide interfaces that are transparent to upper layer,
// thus the below delivers several fully-packed interfaces

func InitNeuron(url string) (*Controller, error) {
	// TODO
	if url == "" {
		url = "nats://152.136.134.100:4222"
	}

	c := &Controller{}

	nc, err := IConnect(url)
	if err != nil {
		_ = fmt.Errorf("interneuron connection failed\n")
		return c, err
	}

	js, err := nc.IJetStream(MaxWait(10 * time.Second))
	if err != nil {
		_ = fmt.Errorf("interneuron jetstream failed\n")
		return c, err
	}

	c.nc = nc
	c.js = js
	return c, nil
}

func (c *Controller) CloseNeuron() {
	c.nc.IClose()
}

func (c *Controller) Pub(msgString string, cfg *PubSubConfig) error {
	js := c.js
	var err error

	// detect empty config
	if cfg == nil || cfg.Interneuron == _EMPTY_ {
		err = fmt.Errorf("FATAL: pub-sub config lost\n")
		return err
	}
	info, _ := js.IStreamInfo(cfg.Interneuron)
	if cfg.DeletePrevious && info != nil {
		err = js.IDeleteStream(cfg.Interneuron)
		if err != nil {
			return err
		}
	}

	switch cfg.Mode {
	case PeerToPeer:
		_, err = js.IAddStreamOneConsumer(cfg.Interneuron, cfg.Interneuron)
	case Broadcast, _EMPTY_:
		_, err = js.IAddStream(cfg.Interneuron, cfg.Interneuron)
	default:
		err = fmt.Errorf("illegal publish mode: %v\n", cfg.Mode)
	}
	if err != nil {
		return err
	}

	switch cfg.Integrity {
	case ExactlyOnce, AtLeastOnce, _EMPTY_:
		_, err = js.IPublish(cfg.Interneuron, []byte(msgString))
	default:
		err = fmt.Errorf("illegal publish integrity policy: %v\n", cfg.Integrity)
	}
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) Sub(cfg *PubSubConfig) (string, error) {
	js := c.js
	var err error
	var msgString string

	// detect empty config
	if cfg == nil || cfg.Interneuron == _EMPTY_ {
		err = fmt.Errorf("FATAL: pub-sub config lost\n")
		return _EMPTY_, err
	}

	switch cfg.Integrity {
	case ExactlyOnce:
		_, err = js.ISubscribeLastMsg(cfg.Interneuron, func(msg *Msg) {
			msgString += string(msg.Data)
		})
	case AtLeastOnce, _EMPTY_:
		_, err = js.ISubscribe(cfg.Interneuron, func(msg *Msg) {
			msgString += string(msg.Data)
		})
	default:
		err = fmt.Errorf("illegal publish integrity policy: %v\n", cfg.Integrity)
	}
	if err != nil {
		return _EMPTY_, err
	}

	return msgString, nil
}
