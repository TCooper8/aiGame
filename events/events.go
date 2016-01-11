package events

import (
  "reflect"
)

type AuthRegisterReply struct {
  success   bool
  err       string
}

type AuthRegister struct {
  Username  string
  Password  string
  Email     string
}

type EventRequest struct {
  Request interface{}
  Reply   <-chan interface{}
}

var typeMapping map[string] *reflect.Type
var replyMapping map[string] *reflect.Type

func init() {
  typeMapping[AuthRegister.Name()] = AuthRegister
  replyMapping[AuthRegister.Name()] = AuthRegisterReply
}
