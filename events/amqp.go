package events

import (
  "errors"
  "github.com/tcooper8/aiGame/logging"
  "github.com/streadway/amqp"
  "code.google.com/p/go-uuid/uuid"
  //"time"
)

type AmqpEventMapping struct {
  EndPoint        string
  EventType       string
  Exchange        string
  ReqQueue        string
  privateQueue    string
  respListenQueue string
}

type connInfo struct {
  conn    *amqp.Connection
  ch      *amqp.Channel
  mapping *AmqpEventMapping
}

type AmqpPool struct {
  log           *logging.Log
  privateQueues map[string] string
  connMap       map[string] *connInfo
  cancelToken   chan bool
  taskMap       map[string] chan interface{}
  changes       chan interface{}
}

// Get mapping change

type GetMappingReply struct {
  mapping *AmqpEventMapping
  ok      bool
}

type getMappingMsg struct {
  eventType string
  reply     chan<- *GetMappingReply
}

// Get connInfo change

type GetConnReply struct {
  connInfo  *connInfo
  ok        bool
}

type getConnMsg struct {
  eventType string
}

// Set mapping change

type SetMappingReply struct {
  err error
}

type setMappingMsg struct {
  mapping *AmqpEventMapping
  reply   chan<- *SetMappingReply
}

// AmqpPool methods

func NewAmqpPool(name string, logLevel int) (*AmqpPool, error) {
  pool := AmqpPool{
    log           : logging.New(name + ":Amqp", logLevel),
    privateQueues : make(map[string] string),
    connMap       : make(map[string] *connInfo),
    cancelToken   : make(chan bool),
    taskMap       : make(map[string] chan interface{}),
    changes       : make(chan interface{}, 16),
  }

  pool.loadDefaultMappings()

  go pool.handleChanges()

  return &pool, nil
}

func (pool *AmqpPool) consumeResponseEvent(eventType string) error {
  eventType, ok := events.GetType(eventType)
  if !ok {
    return errors.New("Invalid event type")
  }

  respType, ok := events.GetResponseType(eventType)
  if !ok {
    return errors.New("Invalid response event type")
  }

  connInfo, err := pool.GetConnInf

  go func() {

  }()

  return nil
}

func (pool *AmqpPool) loadDefaultMappings() {
  pool.setMapping(
    &AmqpEventMapping{
      EndPoint        : "amqp://localhost:5672/",
      EventType       : "AuthRegister",
      Exchange        : "auth",
      ReqQueue        : "auth",
      respListenQueue : "auth.register.response",
      privateQueue    : uuid.New(),
    },
  )
}

func (pool *AmqpPool) handleChanges() {
  log := pool.log
  changes := pool.changes

  for change := range changes {
    switch msg := change.(type) {
    default:
      // Unknown message, log a warning.
      log.Warn("Received unexpected change: %T as %s", msg, msg)

    case setMappingMsg:
      // This is a request for setting a new mapping.
      err := pool.setMapping(msg.mapping)
      msg.reply <- &SetMappingReply{
        err : err,
      }

    case getMappingMsg:
      // This is a request to get an existing mapping.
      val, ok := pool.getMapping(msg.eventType)

      msg.reply <- &GetMappingReply{
        mapping : val,
        ok      : ok,
      }
    }
  }
}

func (pool *AmqpPool) GetConn(eventType string) (*connInfo, bool) {
  reply := make(chan *GetConnReply)

  change := getConnMsg{
    eventType: eventType,
  }

  pool.changes <- change

  resp := <-reply
  return resp.connInfo, resp.ok
}

func (pool *AmqpPool) getConn(eventType string) (*connInfo, bool) {
  return pool.connMap[eventType]
}

func (pool *AmqpPool) GetMapping(eventType string) (*AmqpEventMapping, bool) {
  reply := make(chan *GetMappingReply)

  change := getMappingMsg{
    eventType : eventType,
    reply     : reply,
  }

  pool.changes <- change

  resp := <-reply
  return resp.mapping, resp.ok
}

func (pool *AmqpPool) getMapping(eventType string) (*AmqpEventMapping, bool) {
  val, ok := pool.connMap[eventType]
  if !ok {
    return nil, ok
  }

  return val.mapping, ok
}

func (pool *AmqpPool) setMapping(mapping *AmqpEventMapping) error {
  eventTypes := mapping.EventType
  endPoint := mapping.EndPoint

  conn, err := amqp.Dial(endPoint)
  if err != nil {
    return err
  }

  channel, err := conn.Channel()
  if err != nil {
    return err
  }

  go pool.consumeEvent(eventType)

  connInfo := connInfo{
    conn    : conn,
    ch      : channel,
    mapping : mapping,
  }

  pool.connMap[eventType] = &connInfo

  return nil
}
