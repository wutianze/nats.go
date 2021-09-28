module github.com/nats-io/nats.go

go 1.16

require (
	github.com/nats-io/nats.go/encoders v0.0.0-1c80ab614b9f2e8ed645a1cab8e3b535989b1b44
	github.com/nats-io/nkeys v0.3.0
	github.com/nats-io/nuid v1.0.1
)

replace github.com/nats-io/nats.go => ./

replace github.com/nats-io/nats.go/encoders => ./encoders
