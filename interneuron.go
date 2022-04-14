package nats

type IJetStream interface {
	IStreamInfo(stream string) (*StreamInfo, error)

	IAddStream(stream string, subj string) (*StreamInfo, error)

	IAddStreamOneConsumer(stream string, subj string) (*StreamInfo, error)

	IPurgeStream(stream string) error

	IDeleteStream(name string) error

	IPublish(subj string, data []byte) (*PubAck, error)

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
func (js *js) IPublish(subj string, data []byte) (*PubAck, error) {
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
