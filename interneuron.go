package nats

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
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

	StreamResponsePrefix = "StreamResponsePrefix."

	Broadcast  = "broadcast"
	PeerToPeer = "peer-to-peer"

	ExactlyOnce = "exactly-once"
	AtLeastOnce = "at-least-once"
)

type PubSubConfig struct {
	Topic string `json:"topic"`

	Mode      string `json:"mode"`
	Integrity string `json:"integrity"`
	Latency   string `json:"latency"`

	DeletePrevious bool `json:"delete-previous"`
}

type JsRequestData struct {
	RespSubj string `json:"respSubj"`
	Data     []byte `json:"data"`
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

	IRequest(subj string, data []byte, timeout time.Duration) ([]byte, error)

	IResponse(subj string, data []byte) ([]byte, error)
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
	return js.Subscribe(subj, cb)
}

// ISubscribeLastMsg subscribes exactly the last message from a subject
func (js *js) ISubscribeLastMsg(subj string, cb MsgHandler) (*Subscription, error) {
	return js.Subscribe(subj, cb, DeliverLast())
}

// JetStream do NOT provide request/response method, thus we implement these methods below

func (js *js) IRequest(subj string, data []byte, timeout time.Duration) ([]byte, error) {
	var err error

	timeStamp := time.Now().UnixNano()
	// resp subj format: prefix.timestamp.subj
	respSubj := StreamResponsePrefix + strconv.FormatInt(timeStamp, 10) + "." + subj
	jsReqData := &JsRequestData{
		RespSubj: respSubj,
		Data:     data,
	}

	var respData []byte
	_, err = js.UpdateStream(&StreamConfig{
		Name:     subj,
		Subjects: []string{subj, respSubj},
	})
	if err != nil {
		return nil, err
	}

	if _, err = js.IPublish(subj, jsReqData); err != nil {
		return nil, err
	}

	// TODO duration
	for {
		time.Sleep(time.Millisecond * 50)
		_, err = js.ISubscribe(respSubj, func(m *Msg) {
			//fmt.Printf("response: \"%v\"\n", respData)
			respData = m.Data
		})
		if respData != nil {

			if err = json.Unmarshal(respData, &respData); err != nil {
				return nil, err
			}
			break
		}
	}

	if err != nil {
		return nil, err
	}

	return respData, nil

}

func (js *js) IResponse(subj string, data []byte) ([]byte, error) {
	var err error

	var jsReqData JsRequestData

	for {
		var rawData []byte
		_, err = js.ISubscribe(subj, func(m *Msg) {
			rawData = m.Data
		})
		if rawData != nil {
			err = json.Unmarshal(rawData, &jsReqData)
			break
		}
		time.Sleep(time.Millisecond * 50)
	}

	if err != nil {
		return nil, err
	}
	//fmt.Printf("data: \"%v\", respSubj: \"%v\"\n", jsReqData.Data, jsReqData.RespSubj)
	subData := jsReqData.Data
	respSubj := jsReqData.RespSubj

	// TODO response data RPC
	if _, err = js.IPublish(respSubj, data); err != nil {
		return nil, err
	}

	return subData, nil
}

// The interneuron.go needs to provide interfaces that are transparent to upper layer,
// thus the below delivers several fully-packed interfaces

func InitNeuron(url string) (*Controller, error) {
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

func (c *Controller) Decode(data []byte, vPtr interface{}) (err error) {
	switch arg := vPtr.(type) {
	case *string:
		str := string(data)
		if strings.HasPrefix(str, `"`) && strings.HasSuffix(str, `"`) {
			*arg = str[1 : len(str)-1]
		} else {
			*arg = str
		}
	case *[]byte:
		*arg = data
	default:
		err = json.Unmarshal(data, arg)
	}
	return err
}

// Pub
// You can have 'exactly-once' quality of service by the JetStream publishing application
// inserting a unique publication ID in a header field of the message.
func (c *Controller) Pub(msg interface{}, cfg *PubSubConfig) error {
	js := c.js
	var err error

	// detect empty config
	if cfg == nil || cfg.Topic == _EMPTY_ {
		err = fmt.Errorf("FATAL: pub-sub config lost\n")
		return err
	}
	info, _ := js.IStreamInfo(cfg.Topic)
	if cfg.DeletePrevious && info != nil {
		err = js.IDeleteStream(cfg.Topic)
		if err != nil {
			return err
		}
	}

	switch cfg.Mode {
	case PeerToPeer:
		_, err = js.IAddStreamOneConsumer(cfg.Topic, cfg.Topic)
	case Broadcast, _EMPTY_:
		_, err = js.IAddStream(cfg.Topic, cfg.Topic)
	default:
		err = fmt.Errorf("illegal publish mode: %v\n", cfg.Mode)
	}
	if err != nil {
		return err
	}

	switch cfg.Integrity {
	case ExactlyOnce, AtLeastOnce, _EMPTY_:
		_, err = js.IPublish(cfg.Topic, msg)
	default:
		err = fmt.Errorf("illegal publish integrity policy: %v\n", cfg.Integrity)
	}
	if err != nil {
		return err
	}

	return nil
}

func (c *Controller) Sub(cfg *PubSubConfig, oData interface{}, cb ...MsgHandler) error {
	js := c.js
	var err error
	var SubCB func(msg *Msg)

	// detect empty config
	if cfg == nil || cfg.Topic == _EMPTY_ {
		err = fmt.Errorf("FATAL: pub-sub config lost\n")
		return err
	}

	// msg handler provided by caller
	if len(cb) > 0 {
		argType, numArgs := argInfo(cb)
		if argType == nil || numArgs != 1 {
			return errors.New("nats: Handler requires exactly one argument")
		}
		cbValue := reflect.ValueOf(cb)

		SubCB = func(msg *Msg) {
			var oV []reflect.Value
			var oPtr reflect.Value
			if argType.Kind() != reflect.Ptr {
				oPtr = reflect.New(argType)
			} else {
				oPtr = reflect.New(argType.Elem())
			}
			if err := c.Decode(msg.Data, oData); err != nil {
				return
			}
			if argType.Kind() != reflect.Ptr {
				oPtr = reflect.Indirect(oPtr)
			}

			// Callback Arity
			switch numArgs {
			case 1:
				oV = []reflect.Value{oPtr}
			case 2:
				subV := reflect.ValueOf(msg.Subject)
				oV = []reflect.Value{subV, oPtr}
			case 3:
				subV := reflect.ValueOf(msg.Subject)
				replyV := reflect.ValueOf(msg.Reply)
				oV = []reflect.Value{subV, replyV, oPtr}
			}

			cbValue.Call(oV)
		}
	} else {
		SubCB = func(msg *Msg) {
			err = c.Decode(msg.Data, oData)
			if err != nil {
				fmt.Printf("Unexpected error2: %v\n", err)
				return
			}
		}
	}

	switch cfg.Integrity {
	case ExactlyOnce:
		_, err = js.ISubscribeLastMsg(cfg.Topic, SubCB)
	case AtLeastOnce, _EMPTY_:
		_, err = js.ISubscribe(cfg.Topic, SubCB)
	default:
		err = fmt.Errorf("illegal publish integrity policy: %v\n", cfg.Integrity)
	}

	return err
}
