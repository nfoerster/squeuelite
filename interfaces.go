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
	Close() error
}

const (
	READY int64 = iota
	LOCKED
	FAILED
)

// SubscribeQueue is a small interface works with
// callback and is easer for me to comprehend
// modelled after nats
type SubscribeQueue interface {
	Put([]byte) error
	Subscribe(func(msg *PMessage) error) error
	Close() error
}

type PMessage struct {
	MessageID int64
	Status    int64
	InTime    int64
	LockTime  int64
	DoneTime  int64
	Data      []byte
}
