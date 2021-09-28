// Deprecated: use wallyqs/old-nats-encoders.go instead please
module github.com/nats-io/nats.go/encoders

go 1.17

require (
	github.com/golang/protobuf v1.5.0
	github.com/nats-io/nats.go 8a1a9dfcb705bd30b5a9da4b8250e0d4a31aa7e1
	google.golang.org/protobuf v1.27.1
)

require (
	github.com/nats-io/nats-server/v2 v2.6.1 // indirect
	github.com/nats-io/nkeys v0.3.0 // indirect
	github.com/nats-io/nuid v1.0.1 // indirect
	golang.org/x/crypto v0.0.0-20210616213533-5ff15b29337e // indirect
)

replace github.com/nats-io/nats.go => ../
replace github.com/nats-io/nats.go/encoders => ./
