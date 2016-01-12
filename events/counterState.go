package events

type Counter struct {
  n   int
  inc chan int
  get chan (chan int)
}

func NewCounter() *Counter {
  inc := make(chan int, 128)
  get := make(chan chan int)

  counter := Counter{
    n   : 0,
    inc : inc,
    get : get,
  }

  go func() {
    select {
    case i := <-inc:
      counter.n += i

    case reply := <-get:
      reply <- counter.n
    }
  }()

  return &counter
}

func (counter *Counter) Inc(i int) {
  counter.inc <- i
}

func (counter *Counter) Get() int {
  reply := make(chan int)
  counter.get <- reply

  return <-reply
}

func (counter *Counter) Close() {
  close(counter.inc)
  close(counter.get)
}
