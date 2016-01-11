package logging

import (
  "log"
  "fmt"
  "time"
)

const (
  FATAL = 60
  ERROR = 50
  WARN  = 40
  INFO  = 30
  DEBUG = 20
  TRACE = 10
)

type Log struct {
  name  string
  level int
}

func New (name string, level int) *Log {
  logger := Log{
    name,
    level,
  }

  return &logger
}

func (logger *Log) post(level int, format string, args ...interface{}) {
  if (level >= logger.level) {
    log.Printf(
      "%s\t | %s\t | %s\t | %s\t",
      time.Now().Format(time.RFC822),
      logger.name,
      level,
      fmt.Sprintf(format, args...),
    )
  }
}

func (logger *Log) Fatal(format string, args ...interface{}) {
  logger.post(FATAL, format, args...)
}

func (logger *Log) Error(format string, args ...interface{}) {
  logger.post(ERROR, format, args...)
}

func (logger *Log) Warn(format string, args ...interface{}) {
  logger.post(WARN, format, args...)
}

func (logger *Log) Info(format string, args ...interface{}) {
  logger.post(INFO, format, args...)
}

func (logger *Log) Debug(format string, args ...interface{}) {
  logger.post(DEBUG, format, args...)
}

func (logger *Log) Trace(format string, args ...interface{}) {
  logger.post(TRACE, format, args...)
}
