package main

import (
  "errors"
  "github.com/tcooper8/aiGame/logging"
  "github.com/tcooper8/aiGame/eventBus"
)

const (
  INVALID_MSG = "Invalid message"
  NOT_IMPLEMENTED = "Method not implemented"
)

func consumeRegister(stream <-chan *EventRequest) {
  for event := stream {

  }
}

func consumeRegister(msg *events.AuthRegister) *AuthRegisterReply {
  return &AuthRegisterReply{
    success : false,
    err     : NOT_IMPLEMENTED,
  }
}

func main() {
  bus, err := events.NewAmqpPool("amqpPool", logging.INFO)
  if err != nil {
    panic(err)
  }

  respondsOn := events.AuthGroup
  respondsOn := make(map[string], chan interface{})

  bus.SetMapping(&AmqpEventMapping{
    EndPoint    : "amqp://localhost:5672/",
    RespondsOn  : make(map[string],
  })
}
