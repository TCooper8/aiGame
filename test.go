package main

import (
  "github.com/tcooper8/aiGame/logging"
  "github.com/tcooper8/aiGame/events"
)

const (
  INVALID_MSG = "Invalid message"
  NOT_IMPLEMENTED = "Method not implemented"
)

func consumeRegister(msg *events.AuthRegister) *events.AuthRegisterReply {
  return &events.AuthRegisterReply{
    Success : false,
    Err     : NOT_IMPLEMENTED,
  }
}

func main() {
  log := logging.New("service", logging.INFO)

  bus, err := events.NewAmqpPool("amqpPool", logging.INFO)
  if err != nil {
    panic(err)
  }

  consumer := make(chan *events.EventRequest)

  bus.SetConsumer("auth.register", consumer)
  bus.SetConsumer("auth.register.response", consumer)

  log.Info("Running")

  err = bus.SetMapping(&events.AmqpEventMapping{
    EndPoint    : "amqp://localhost:5672/",
    EventType   : "auth.register",
    Exchange    : "auth",
    ReqQueue    : "auth.register",
  })
  if err != nil {
    log.Error("SetMapping error: %s", err)
  }

  err = bus.SetMapping(&events.AmqpEventMapping{
    EndPoint    : "amqp://localhost:5672/",
    EventType   : "auth.register.response",
    Exchange    : "auth",
    ReqQueue    : "auth.register.response",
  })
  if err != nil {
    log.Error("SetMapping error: %s", err)
  }

  for req := range(consumer) {
    switch msg := req.Request.(type) {
    default:
      log.Warn("Received unhandled msg: %T %s", msg, msg)

    case *events.AuthRegisterReply:
      log.Info("Got reply %s", msg)

    case events.AuthRegister:
      req.Reply <- consumeRegister(&msg)

    case *events.AuthRegister:
      req.Reply <- consumeRegister(msg)
    }
  }
}
