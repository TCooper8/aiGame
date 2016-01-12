package events

import (
  "errors"
  "reflect"
  "encoding/json"
  "github.com/tcooper8/aiGame/logging"
  "github.com/streadway/amqp"
  //"code.google.com/p/go-uuid/uuid"
  "time"
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
  conn        *amqp.Connection
  ch          *amqp.Channel
  mapping     *AmqpEventMapping
  cancelToken <-chan bool
}

type AmqpPool struct {
  log           *logging.Log
  privateQueues map[string] string
  connMap       map[string] *connInfo
  consumerMap   map[string] chan<- *EventRequest
  endPointCache map[string] *amqp.Connection
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

type getConnReply struct {
  connInfo  *connInfo
  ok        bool
}

type getConnMsg struct {
  eventType string
  reply     chan<- *getConnReply
}

// Set mapping change

type setMappingReply struct {
  err error
}

type setMappingMsg struct {
  mapping *AmqpEventMapping
  reply   chan<- *setMappingReply
}

// Get consumer

type getConsumerReply struct {
  consumer  chan<- *EventRequest
  ok        bool
}

type getConsumerMsg struct {
  eventType string
  reply     chan<- *getConsumerReply
}

// Set consumer

type setConsumerMsg struct {
  consumer  chan<- *EventRequest
  eventType string
}

// AmqpPool methods

func NewAmqpPool(name string, logLevel int) (*AmqpPool, error) {
  pool := AmqpPool{
    log           : logging.New(name + ":Amqp", logLevel),
    privateQueues : make(map[string] string),
    connMap       : make(map[string] *connInfo),
    consumerMap   : make(map[string] chan<- *EventRequest),
    endPointCache : make(map[string] *amqp.Connection),
    cancelToken   : make(chan bool),
    taskMap       : make(map[string] chan interface{}),
    changes       : make(chan interface{}),
  }

  pool.loadDefaultMappings()

  go pool.handleChanges()

  return &pool, nil
}

func (pool *AmqpPool) consumeResponseEvent(eventType string) error {
  log := pool.log

  eventTypeType, ok := GetType(eventType)
  if !ok {
    return errors.New("Invalid event type")
  }

  //respType, ok := GetResponseType(eventType)
  //if !ok {
  //  return errors.New("Invalid response event type")
  //}

  connInfo, ok := pool.getConn(eventType)
  if !ok {
    log.Warn("Could not get connInfo")
    return errors.New("No mapping found for event type")
  }
  log.Info("Got conn info")

  //conn := connInfo.conn
  ch := connInfo.ch
  mapping := connInfo.mapping

  // Declare the exchange and queue for the event.
  log.Info("Creating exchange %s", mapping.Exchange)
  err := ch.ExchangeDeclare(
    mapping.Exchange, // Exchange name
    "topic",          // Topical
    true,             // . . .
    false,
    false,
    false,
    nil,
  )
  if err != nil {
    return err
  }

  // Declare the queue for the event.
  log.Info("Creating queue %s", mapping.ReqQueue)
  _, err = ch.QueueDeclare(
    mapping.ReqQueue,
    false,
    false,
    false,
    false,
    nil,
  )
  if err != nil {
    return err
  }

  // Declare the response queue.
  log.Info("Creating queue %s", mapping.ReqQueue)
  _, err = ch.QueueDeclare(
    mapping.respListenQueue,
    false,
    false,
    false,
    false,
    nil,
  )
  if err != nil {
    return err
  }

  // Bind the event to the appropriate routing keys.
  log.Info(
    "Binding routing key %s to exchange %s and queue %s",
    eventType,
    mapping.Exchange,
    mapping.ReqQueue,
  )
  err = ch.QueueBind(
    mapping.ReqQueue,
    eventType,
    mapping.Exchange,
    false,
    nil,
  )
  if err != nil {
    return err
  }

  // Consume the deliveries from this channel.
  log.Info("Pulling deliveries from event bus")
  deliveries, err := ch.Consume(
    mapping.ReqQueue,
    "",
    false,
    false,
    false,
    false,
    nil,
  )
  if err != nil {
    return err
  }

  err = ch.Qos(1, 0, false)
  if err != nil {
    return err
  }

  consumer, ok := pool.getConsumer(eventType)
  if !ok {
    return errors.New("No consumer defined for eventType")
  }

  // Spin up the goroutine to consume the events.
  go func() {
    log.Info("Consuming messages from %s", mapping.ReqQueue)

    cancelToken := connInfo.cancelToken

    counter := NewCounter()

    for {
      select {
      case <-cancelToken:
        log.Info("Stopping %s", mapping.EndPoint)
        return

      case letter := <-deliveries:
        req := reflect.New(eventTypeType).Interface()
        log.Info("Unmarshalling %T: %s", req, req)

        err := json.Unmarshal(letter.Body, req)
        if err != nil {
          log.Warn("Unable to unmarshal letter %s: %s", letter.Body, err)
          continue
        }
        log.Info("Unmarshalled message %s", req)

        counter.Inc(1)

        reply := make(chan interface{})
        handle := &EventRequest{
          Request : req,
          Reply   : reply,
        }
        log.Info("Publishing %s to consumer")
        consumer <- handle

        go func() {
          log.Info("Waiting for consumer")
          resp := <-reply
          log.Info("Got reply from consumer")
          close(reply)

          // Acknowledge the letter.
          letter.Ack(false)
          counter.Inc(-1)
          log.Info("Acked letter")


          //if reflect.TypeOf(resp) != respType {
          //  log.Warn("Response is invalid type %s", resp)
          //  return
          //}

          // Get the payload in json format.
          json, err := json.Marshal(resp)
          if err != nil {
            log.Warn("Cannot stringify response: %s", err)
            return
          }

          // Publish the response to the requestor.
          err = ch.Publish(
            "",             // Default exchange
            letter.ReplyTo, // Direct queue
            false,
            false,
            amqp.Publishing{
              ContentType   : "text/plain",
              Body          : json,
              CorrelationId : letter.CorrelationId,
            },
          )
          if err != nil {
            log.Warn("Publishing error %s", err)
          }

          log.Info("Publishing response %s", resp)

          // Also, publish the response to the response queue.
          err = ch.Publish(
            mapping.Exchange,
            mapping.respListenQueue,
            false,
            false,
            amqp.Publishing{
              ContentType : "text/plain",
              Body        : json,
            },
          )
          if err != nil {
            log.Warn("Publishing to response channel error %s", err);
          }

          log.Info("Publishing response %s", resp)
        }()
      }

      // Block until there are less than the max number of working events.
      for counter.Get() > 32 {
        time.Sleep(16 * time.Millisecond)
      }
    }
  }()

  return nil
}

func (pool *AmqpPool) loadDefaultMappings() {
  //pool.setMapping(
  //  &AmqpEventMapping{
  //    EndPoint        : "amqp://localhost:5672/",
  //    EventType       : "AuthRegister",
  //    Exchange        : "auth",
  //    ReqQueue        : "auth",
  //    respListenQueue : "auth.register.response",
  //    privateQueue    : uuid.New(),
  //  },
  //)
}

func (pool *AmqpPool) handleChanges() {
  log := pool.log
  changes := pool.changes

  for change := range changes {
    log.Info("Got change %s", change)
    switch msg := change.(type) {
    default:
      // Unknown message, log a warning.
      log.Warn("Received unexpected change: %T as %s", msg, msg)

    case getConnMsg:
      log.Info("Got getConnMsg")
      val, ok := pool.getConn(msg.eventType)
      msg.reply <- &getConnReply{
        connInfo  : val,
        ok        : ok,
      }

    case setMappingMsg:
      log.Info("Got setMappingMsg")
      // This is a request for setting a new mapping.
      err := pool.setMapping(msg.mapping)
      msg.reply <- &setMappingReply{
        err : err,
      }

    case getMappingMsg:
      // This is a request to get an existing mapping.
      val, ok := pool.getMapping(msg.eventType)

      msg.reply <- &GetMappingReply{
        mapping : val,
        ok      : ok,
      }

    case setConsumerMsg:
      pool.setConsumer(msg.eventType, msg.consumer)

    case getConsumerMsg:
      val, ok := pool.getConsumer(msg.eventType)
      msg.reply <- &getConsumerReply{
        consumer  : val,
        ok        : ok,
      }
    }
  }

  log.Info("Stopped listening for changes")
}

func (pool *AmqpPool) GetConn(eventType string) (*connInfo, bool) {
  reply := make(chan *getConnReply)

  change := getConnMsg{
    eventType: eventType,
  }

  pool.log.Info("Pushing change to queue")
  pool.changes <- change

  resp := <-reply
  pool.log.Info("Got reply from queue")
  return resp.connInfo, resp.ok
}

func (pool *AmqpPool) getConn(eventType string) (*connInfo, bool) {
  val, ok := pool.connMap[eventType]
  return val, ok
}

func (pool *AmqpPool) getConsumer(eventType string) (chan<- *EventRequest, bool) {
  val, ok := pool.consumerMap[eventType]
  return val, ok
}

func (pool *AmqpPool) GetConsumer(eventType string) (chan<- *EventRequest, bool) {
  reply := make(chan *getConsumerReply)

  change := getConsumerMsg{
    eventType : eventType,
    reply     : reply,
  }

  pool.changes <- change

  resp := <-reply
  return resp.consumer, resp.ok
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

func (pool *AmqpPool) setConsumer(eventType string, consumer chan<- *EventRequest) {
  pool.consumerMap[eventType] = consumer
}

func (pool *AmqpPool) SetConsumer(eventType string, consumer chan<- *EventRequest) {
  change := setConsumerMsg{
    consumer  : consumer,
    eventType : eventType,
  }
  pool.changes <- change
}

func (pool *AmqpPool) setMapping(mapping *AmqpEventMapping) error {
  log := pool.log
  eventType := mapping.EventType
  endPoint := mapping.EndPoint

  log.Info("Setting mapping for %s ...", eventType)

  // Set the respListenQueue on the mapping.
  mapping.respListenQueue = eventType + ".response"

  var err error
  conn, ok := pool.endPointCache[endPoint]
  if !ok {
    conn, err = amqp.Dial(endPoint)
    log.Warn("%s not cached", endPoint)
    if err != nil {
      log.Warn("Could not dial %s", endPoint)
      return err
    }
  }
  log.Info("Connected, creating channel")

  channel, err := conn.Channel()
  if err != nil {
    return err
  }
  log.Info("Created channel")


  connInfo := connInfo{
    conn    : conn,
    ch      : channel,
    mapping : mapping,
  }

  pool.connMap[eventType] = &connInfo

  err = pool.consumeResponseEvent(eventType)
  if err != nil {
    log.Warn("Could not consume response event: %s", err)
    return err
  }
  log.Info("Consuming response event")

  return nil
}

func (pool *AmqpPool) SetMapping(mapping *AmqpEventMapping) error {
  reply := make(chan *setMappingReply)

  pool.changes <- setMappingMsg{
    mapping : mapping,
    reply   : reply,
  }

  resp := <-reply
  return resp.err
}
