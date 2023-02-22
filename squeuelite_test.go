package squeuelite

import (
	"database/sql"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
)

func TestBasicFunctionality(t *testing.T) {
	//if on FS, delete old queue
	defer os.Remove(".queue.db")
	for _, env := range []bool{true, false} {
		queue, err := NewSQueueLite(".queue", env, 2)
		if err != nil {
			t.Fatal(err)
		}
		err = queue.Put("Test1")
		if err != nil {
			t.Fatal(err)
		}
		err = queue.Put("Test2")
		if err != nil {
			t.Fatal(err)
		}
		msg, err := queue.Peek()
		if err != nil {
			t.Fatal(err)
		}
		if msg.Data != "Test1" {
			t.Fatalf("First message payload should be Test1:%v", msg.Data)
		}
		msg, err = queue.Peek()
		if err != nil {
			t.Fatal(err)
		}
		if msg.Data != "Test1" {
			t.Fatalf("First message payload should be still Test1:%v", msg.Data)
		}
		err = queue.Done(msg.MessageID)
		if err != nil {
			t.Fatal(err)
		}
		msg, err = queue.Peek()
		if err != nil {
			t.Fatal(err)
		}
		if msg.Data != "Test2" {
			t.Fatalf("Second message payload should be Test2:%v", msg.Data)
		}
		size, err := queue.Qsize()
		if err != nil {
			t.Fatal(err)
		}
		if size != 1 {
			t.Fatalf("Now only one message should be not DONE/FAILED:%v", size)
		}
		isEmpty, err := queue.Empty()
		if err != nil {
			t.Fatal(err)
		}
		if isEmpty {
			t.Fatal("Queue should not be empty")
		}
		isFull, err := queue.Full()
		if err != nil {
			t.Fatal(err)
		}
		if isFull {
			t.Fatal("Queue should not be full")
		}
		err = queue.Put("Test3")
		if err != nil {
			t.Fatal(err)
		}
		isFull, err = queue.Full()
		if err != nil {
			t.Fatal(err)
		}
		if !isFull {
			t.Fatal("Queue should be full")
		}
		msg, err = queue.Peek()
		if err != nil {
			t.Fatal(err)
		}
		if msg.Data != "Test2" {
			t.Fatalf("Second message payload should be Test2:%v", msg.Data)
		}
		msg2, err := queue.Get(msg.MessageID)
		if err != nil {
			t.Fatal(err)
		}
		if msg.MessageID != msg2.MessageID {
			t.Fatalf("Messages should be same from Peek and Get by msg id:%+v<->%+v", msg, msg2)
		}
		err = queue.Done(msg.MessageID)
		if err != nil {
			t.Fatal(err)
		}
		msg, err = queue.Peek()
		if err != nil {
			t.Fatal(err)
		}
		err = queue.Done(msg.MessageID)
		if err != nil {
			t.Fatal(err)
		}

		err = queue.Prune()
		if err != nil {
			t.Fatal(err)
		}
		isEmpty, err = queue.Empty()
		if err != nil {
			t.Fatal(err)
		}
		if !isEmpty {
			t.Fatal("Queue should be empty")
		}
		queue.Close()
	}
}

func TestBasicPeakBlock(t *testing.T) {
	queue, err := NewSQueueLite(".queue", true, 100)
	if err != nil {
		t.Fatal(err)
	}

	go func(queue *SQueueLite) {
		time.Sleep(2 * time.Second)

		err := queue.Put("Test")
		if err != nil {
			t.Fatal(err)
		}
	}(queue)

	msg, err := queue.PeekBlock()
	if err != nil {
		t.Fatal(err)
	}
	if msg.Data != "Test" {
		t.Fatalf("Should be Test:%v", msg.Data)
	}
}

func TestLoad(t *testing.T) {
	defer os.Remove(".queue.db")
	queue, err := NewSQueueLite(".queue", false, 10)
	if err != nil {
		t.Fatal(err)
	}
	err = queue.Put("Test1!")
	if err != nil {
		t.Fatal(err)
	}
	err = queue.Put("Test2!")
	if err != nil {
		t.Fatal(err)
	}
	err = queue.Put("Test3!")
	if err != nil {
		t.Fatal(err)
	}
	err = queue.Close()
	if err != nil {
		t.Fatal(err)
	}

	queue, err = NewSQueueLite(".queue", false, 10)
	if err != nil {
		t.Fatal(err)
	}

	msg, err := queue.Peek()
	if err != nil {
		t.Fatal(err)
	}
	if msg.Data != "Test1!" {
		t.Fatalf("Should be Test1!:%v", msg.Data)
	}
	err = queue.Done(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	msg, err = queue.Peek()
	if err != nil {
		t.Fatal(err)
	}
	if msg.Data != "Test2!" {
		t.Fatalf("Should be Test1!:%v", msg.Data)
	}
	err = queue.Done(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	msg, err = queue.Peek()
	if err != nil {
		t.Fatal(err)
	}
	if msg.Data != "Test3!" {
		t.Fatalf("Should be Test1!:%v", msg.Data)
	}
	err = queue.Done(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	isempty, err := queue.Empty()
	if err != nil {
		t.Fatal(err)
	}
	if !isempty {
		t.Fatal("Queue should be empty")
	}
	err = queue.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestMultithreading(t *testing.T) {
	os.Remove(".queue.db")
	os.Remove(".queue.db-journal")
	queue, err := NewSQueueLiteWithTimeout(".queue", false, 1000, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	defer os.Remove(".queue.db")
	defer os.Remove(".queue.db-journal")
	wg := sync.WaitGroup{}
	//producers
	for i := 0; i < 10; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				content := uuid.New().String()
				err = queue.Put(content)
				if err != nil {
					t.Fatal(err)
				}
			}
			log.Printf("Producer done:%v", i)
			wg.Done()

		}()
		wg.Add(1)
	}
	//consumers PeekingBlock
	for i := 0; i < 5; i++ {
		go func() {
			for {
				msg, err := queue.PeekBlock()
				if err != nil {
					if err == sql.ErrNoRows {
						wg.Done()
						log.Printf("Block Peek Consumer done:%v", i)
						break
					}
				}
				//log.Printf("BlockPeeked msg:%v from cons:%v", msg.MessageID, i)
				err = queue.Done(msg.MessageID)
				if err != nil {
					t.Fatal(err)
				}
			}
		}()
		wg.Add(1)
	}
	//consumers Peeking
	for i := 0; i < 5; i++ {
		go func() {
			for {
				msg, err := queue.Peek()
				if err != nil {
					if err == sql.ErrNoRows {
						wg.Done()
						log.Printf("Peek Consumer done:%v", i)
						break
					}
				}
				//log.Printf("Peeked msg:%v from cons:%v", msg.MessageID, i)
				err = queue.Done(msg.MessageID)
				if err != nil {
					t.Fatal(err)
				}
			}
		}()
		wg.Add(1)
	}
	wg.Wait()
}

func TestRetry(t *testing.T) {
	os.Remove(".queue.db")
	os.Remove(".queue.db-journal")
	queue, err := NewSQueueLite(".queue", true, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	defer os.Remove(".queue.db")
	defer os.Remove(".queue.db-journal")
	err = queue.Put("TestRetry")
	if err != nil {
		t.Fatal(err)
	}

	msg, err := queue.Peek()
	if err != nil {
		t.Fatal(err)
	}

	err = queue.MarkFailed(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}

	_, err = queue.Peek()
	if err == nil {
		t.Fatal("should be an ErrNoRows error")
	} else if err != sql.ErrNoRows {
		t.Fatal(err)
	}

	err = queue.Retry(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	msg, err = queue.Peek()
	if err != nil {
		t.Fatal(err)
	}
	if msg.Data != "TestRetry" {
		t.Fatal("msg.Data should be TestRetry")
	}
	err = queue.Done(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	err = queue.Put("TestRetry2")
	if err != nil {
		t.Fatal(err)
	}
	err = queue.Put("TestRetry3")
	if err != nil {
		t.Fatal(err)
	}
	msg, err = queue.Peek()
	if err != nil {
		t.Fatal(err)
	}
	if msg.Data != "TestRetry2" {
		t.Fatal("msg.Data should be TestRetry2")
	}
	err = queue.Done(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	msg, err = queue.Peek()
	if err != nil {
		t.Fatal(err)
	}
	err = queue.MarkFailed(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}

	err = queue.RetryFailed()
	if err != nil {
		t.Fatal(err)
	}
	msg, err = queue.PeekBlock()
	if err != nil {
		t.Fatal(err)
	}
	if msg.Data != "TestRetry3" {
		t.Fatal("msg.Data should be TestRetry3")
	}
	err = queue.Done(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	msg, err = queue.Peek()
	if err == nil {
		t.Fatal("should be an ErrNoRows error")
	} else if err != sql.ErrNoRows {
		t.Fatal(err)
	}
}
