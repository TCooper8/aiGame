package events

import (
  "reflect"
)

type AuthRegisterReply struct {
  Success   bool
  Err       string
}

type AuthRegister struct {
  Username  string  `json:"username"`
  Password  string  `json:"password"`
  Email     string  `json:"email"`
}

type EventRequest struct {
  Request interface{}
  Reply   chan<- interface{}
}

var typeMapping map[string] reflect.Type
var replyMapping map[string] reflect.Type

func init() {
  typeMapping = make(map[string] reflect.Type)
  replyMapping = make(map[string] reflect.Type)

  v0 := AuthRegister{}
  typeMapping["auth.register"] = reflect.TypeOf(v0)

  v1 := AuthRegisterReply{}
  replyMapping["auth.register"] = reflect.TypeOf(v1)
  typeMapping["auth.register.response"] = reflect.TypeOf(v1)
}

func GetType(eventType string) (reflect.Type, bool) {
  val, ok := typeMapping[eventType]
  return val, ok
}

func GetResponseType(eventType string) (reflect.Type, bool) {
  val, ok := replyMapping[eventType]
  return val, ok
}
