module github.com/nats-io/nats.go

go 1.16

require (
	github.com/nats-io/nkeys v0.3.0
	github.com/nats-io/nuid v1.0.1
	github.com/nats-io/nats.go/encoders 8a1a9dfcb705bd30b5a9da4b8250e0d4a31aa7e1
)

replace github.com/nats-io/nats.go => ./
replace github.com/nats-io/nats.go/encoders => ./encoders
