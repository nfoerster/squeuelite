package squeuelite

// WorkQueue is not a "small" interface so its not very go like
// secondly we have to peek and then acknowledge 
type WorkQueue interface {
	Put([]byte) error 
	Get(int64) (*PMessage, error)
	Peek() (*PMessage, error)
	Done(int64) error
	Empty() (bool, error)
	Qsize() (int64, error)
	Full() (bool, error)
	Prune() error 
}

type Subscriber func([]byte) error 

type Sub interface {
	Subscriber
}

type PMessage struct {
	MessageID int64
	Status    int64
	InTime    int64
	LockTime  int64
	DoneTime  int64
	Data      []byte
}
