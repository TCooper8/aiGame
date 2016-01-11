package cm

import (
  "fmt"
  "log"
  "gopkg.in/mgo.v2"
  "gopkg.in/mgo.v2/bson"
)

type Client struct {
  Uuid string
}

type ClientApp struct {
  Uuid        string
  ClientUuid  string
  Typename    string
}


