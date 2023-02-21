package squeuelite

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestBaseTest(t *testing.T) {
	queue, err := NewSQueueLite("test", true, 2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = queue.Put("Test1")
	if err != nil {
		t.Fatal(err)
	}
	_, err = queue.Put("Test2")
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
	a, err := queue.Done(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if a != 2 {
		t.Fatalf("Last inserted id should be 2:%v", a)
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
	_, err = queue.Put("Test3")
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
	_, err = queue.Done(msg.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	msg, err = queue.Peek()
	if err != nil {
		t.Fatal(err)
	}
	_, err = queue.Done(msg.MessageID)
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
}

func TestSQueueLite_Done(t *testing.T) {
	type fields struct {
		conn    *sql.DB
		maxsize int
	}
	type args struct {
		MessageID string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lq := &SQueueLite{
				conn:    tt.fields.conn,
				maxsize: tt.fields.maxsize,
			}
			got, err := lq.Done(tt.args.MessageID)
			if (err != nil) != tt.wantErr {
				t.Errorf("SQueueLite.Done() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SQueueLite.Done() = %v, want %v", got, tt.want)
			}
		})
	}
}
