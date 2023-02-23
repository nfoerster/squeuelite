package squeuelite

import (
	"bytes"
	"os"
	"testing"
)

func testQueueImplementation(t *testing.T, q WorkQueue) {

	p1 := []byte("Test1")
	p2 := []byte("Test2")
	p3 := []byte("Test3")

	err := q.Put(p1)
	if err != nil {
		t.Fatal(err)
	}
	err = q.Put(p2)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := q.Peek()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg.Data, p1) {
		t.Fatalf("message payload was %v expected %v", msg.Data, p1)
	}
	msg, err = q.Peek()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg.Data, p1) {
		t.Fatalf("message payload was %v expected %v", msg.Data, p1)
	}
	err = q.Done(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	msg, err = q.Peek()
	if err != nil {
		t.Fatal(err)
	}
	if ! bytes.Equal(msg.Data, p2) {
		t.Fatalf("message payload was %v expected %v", msg.Data, p2)
	}
	size, err := q.Qsize()
	if err != nil {
		t.Fatal(err)
	}
	if size != 1 {
		t.Fatalf("Now only one message should be not DONE/FAILED:%v", size)
	}
	isEmpty, err := q.Empty()
	if err != nil {
		t.Fatal(err)
	}
	if isEmpty {
		t.Fatal("Queue should not be empty")
	}
	isFull, err := q.Full()
	if err != nil {
		t.Fatal(err)
	}
	if isFull {
		t.Fatal("Queue should not be full")
	}
	err = q.Put(p3)
	if err != nil {
		t.Fatal(err)
	}
	isFull, err = q.Full()
	if err != nil {
		t.Fatal(err)
	}
	if !isFull {
		t.Fatal("Queue should be full")
	}
	msg, err = q.Peek()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(msg.Data, p2) {
		t.Fatalf("Second message payload is %v expected %v", msg.Data, p2)
	}
	msg2, err := q.Get(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if msg.MessageID != msg2.MessageID {
		t.Fatalf("Messages should be same from Peek and Get by msg id:%+v<->%+v", msg, msg2)
	}
	err = q.Done(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	msg, err = q.Peek()
	if err != nil {
		t.Fatal(err)
	}
	err = q.Done(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}

	err = q.Prune()
	if err != nil {
		t.Fatal(err)
	}
	isEmpty, err = q.Empty()
	if err != nil {
		t.Fatal(err)
	}
	if !isEmpty {
		t.Fatal("Queue should be empty")
	}
}

func TestQueue(t *testing.T) {
	err := os.Remove("test.db")
	if err != nil {
		if _, ok := err.(*os.PathError); !ok {
			t.Error(err)
			t.FailNow()
		}
	}
	q, err := NewPQueue("test.db", 2)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	testQueueImplementation(t, q)
}
