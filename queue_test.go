package squeuelite

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
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
	if !bytes.Equal(msg.Data, p2) {
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

	isEmpty, err = q.Empty()
	if err != nil {
		t.Fatal(err)
	}
	if !isEmpty {
		t.Fatal("Queue should be empty")
	}
}

func TestQueue(t *testing.T) {
	q, err := NewPQueue("test.db", 2)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer os.Remove("test.db")
	defer q.Close()

	testQueueImplementation(t, q)
}

func TestSubscriber(t *testing.T) {
	q, err := NewSQueue("test_sub.db")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	defer q.Close()
	defer os.Remove("test_sub.db")

	nmsg := 10
	go func() {
		for ii := 0; ii < nmsg; ii++ {
			payload := []byte(fmt.Sprintf("Test %v", ii))
			err := q.Put(payload)
			if err != nil {
				t.Error(err)
			}
		}
	}()

	count := 0
	err = q.Subscribe(func(m *PMessage) error {
		fmt.Println(m.Data)
		count++
		return nil
	})
	if err != nil {
		t.Error(err)
	}
	time.Sleep(2 * time.Second)
	if count != nmsg {
		t.Error("did not receive ", nmsg, " messages, was: ", count)
	}
}

func TestMultithreading(t *testing.T) {
	queue, err := NewSQueue("test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	defer os.Remove("test.db")

	maxMsgs := 10
	workers := 10
	atomicCounterPut := 0
	atomicCounterGet := 0

	wg := sync.WaitGroup{}
	//producers
	for i := 0; i < workers; i++ {
		go func() {
			for j := 0; j < maxMsgs; j++ {
				content := uuid.New().String()
				queue.Put([]byte(content))
				atomicCounterPut = atomicCounterPut + 1
			}
			log.Print("producer done")
			wg.Done()

		}()
		wg.Add(1)
	}
	//subscriber is a singleton
	wg.Add(1)
	cancellationChan := make(chan int, 1)
	go func() {
		queue.Subscribe(func(p *PMessage) error {
			atomicCounterGet = atomicCounterGet + 1
			if atomicCounterGet%100 == 0 {
				t.Log(atomicCounterGet)
			}
			if atomicCounterPut == (maxMsgs*workers) && atomicCounterGet >= (maxMsgs*workers) {
				cancellationChan <- 1
			}
			return nil
		})
		<-cancellationChan
		wg.Done()
	}()
	wg.Wait()
}

// func BenchmarkBasic(b *testing.B) {
// 	queue, err := NewSQueue("test.db")
// 	if err != nil {
// 		b.Fatal(err)
// 	}
// 	defer queue.Close()
// 	defer os.Remove("test.db")

// 	maxMsgs := 100
// 	workers := 10
// 	atomicCounterPut := 0
// 	atomicCounterGet := 0

// 	wg := sync.WaitGroup{}
// 	//producers
// 	for i := 0; i < workers; i++ {
// 		go func() {
// 			for j := 0; j < maxMsgs; j++ {
// 				content := uuid.New().String()
// 				queue.Put([]byte(content))
// 				atomicCounterPut = atomicCounterPut + 1
// 			}
// 			log.Print("producer done")
// 			wg.Done()

// 		}()
// 		wg.Add(1)
// 	}
// 	//subscriber is a singleton
// 	wg.Add(1)
// 	cancellationChan := make(chan int, 1)
// 	go func() {
// 		queue.Subscribe(func(p *PMessage) error {
// 			atomicCounterGet = atomicCounterGet + 1
// 			if atomicCounterPut == (maxMsgs*workers) && atomicCounterGet >= (maxMsgs*workers) {
// 				cancellationChan <- 1
// 			}
// 			return nil
// 		})
// 		<-cancellationChan
// 		wg.Done()
// 	}()
// 	wg.Wait()
// }
