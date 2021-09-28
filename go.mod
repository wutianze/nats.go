module github.com/nats-io/nats.go

go 1.16

require (
	github.com/nats-io/nats.go/encoders v0.0.0-00010101000000-000000000000
	github.com/nats-io/nkeys v0.3.0
	github.com/nats-io/nuid v1.0.1
)

replace github.com/nats-io/nats.go => ./

replace github.com/nats-io/nats.go/encoders => github.com/wallyqs/old-nats-encoders.go v0.0.0-20210928102906-55a462a003bd
