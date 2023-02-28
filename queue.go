package squeuelite

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type PQueue struct {
	conn       *sql.DB
	notify     chan int64
	internal   chan *PMessage
	subscribed bool
	queuelen   int64
	maxsize    int64

	insert    *sql.Stmt
	update    *sql.Stmt
	updateall *sql.Stmt
	nextfree  *sql.Stmt
	delete    *sql.Stmt
	selmsg    *sql.Stmt
	selnext   *sql.Stmt
	sizeStmt  *sql.Stmt
}

// PQueue is a persistent queue that can be
// used in two different modes.
//
// It can be used manually with Peek, Get, Done, MarkFailed, Retry
// functions.
//
// Alternatively it can be used with a subscription function
// to get automatically notified after a message is added to
// the queue.
func NewPQueue(connStr string, maxsize int64) (*PQueue, error) {
	conn, err := sql.Open("sqlite3", fmt.Sprintf("%v%v", connStr, "?mode=rw&_sync=2&_auto_vacuum=2&_journal_mode=wal"))
	if err != nil {
		return nil, err
	}

	lq := &PQueue{
		conn:     conn,
		notify:   make(chan int64, 10),
		internal: make(chan *PMessage, 10),
		maxsize:  maxsize,
	}

	if err = lq.init(); err != nil {
		return nil, err
	}

	//prepared statements
	insertStmt, err := lq.conn.Prepare("insert into queue( status, data, in_time ) VALUES ( ?, ? , unixepoch()) returning message_id, status, data")
	if err != nil {
		return nil, err
	}
	lq.insert = insertStmt

	updateStmt, err := lq.conn.Prepare("UPDATE queue SET status = ? WHERE message_id = ?")
	if err != nil {
		return nil, err
	}
	lq.update = updateStmt

	updateAllStmt, err := lq.conn.Prepare(fmt.Sprintf("UPDATE queue SET status = %v WHERE status = %v", READY, FAILED))
	if err != nil {
		return nil, err
	}
	lq.updateall = updateAllStmt

	nextfreeStmt, err := lq.conn.Prepare("UPDATE queue SET status = ? where message_id = (Select message_id from queue where status = ? order by message_id asc limit 1) returning message_id, status, data")
	if err != nil {
		return nil, err
	}
	lq.nextfree = nextfreeStmt

	deleteStmt, err := lq.conn.Prepare("DELETE FROM queue WHERE message_id = ?")
	if err != nil {
		return nil, err
	}
	lq.delete = deleteStmt

	selmsgStmt, err := lq.conn.Prepare("select message_id, status, data from queue where message_id = ?")
	if err != nil {
		return nil, err
	}
	lq.selmsg = selmsgStmt

	selnextStmt, err := lq.conn.Prepare("select message_id, status, data from queue where status = ? order by in_time asc limit 1")
	if err != nil {
		return nil, err
	}
	lq.selnext = selnextStmt

	sizeStmt, err := lq.conn.Prepare("SELECT COUNT(*) FROM Queue WHERE status <> ?")
	if err != nil {
		return nil, err
	}
	lq.sizeStmt = sizeStmt

	// if starting from populated queue we are starting with capacity in notify
	row := conn.QueryRow("select count(*) from queue")
	if row.Err() != nil {
		return nil, row.Err()
	}
	var queuelen int64
	row.Scan(&queuelen)
	if queuelen > 0 {
		lq.notify <- queuelen
	}
	//start notification
	lq.pump()
	return lq, nil
}

func (lq *PQueue) init() error {
	_, err := lq.conn.Exec(`
		CREATE TABLE IF NOT EXISTS queue (
	            message_id  INTEGER PRIMARY KEY
		  , status      INTEGER NOT NULL
	          , in_time     INTEGER NOT NULL default 0
		  , lock_time   INTEGER NOT NULL default 0 
		  , done_time   INTEGER NOT NULL default 0 
	          , data        TEXT    NOT NULL
		)`)

	if err != nil {
		return err
	}

	return nil
}

func (lq *PQueue) next() (*PMessage, error) {
	row := lq.nextfree.QueryRow(LOCKED, READY)
	if row.Err() != nil {
		return nil, row.Err()
	}

	msg := &PMessage{}
	err := row.Scan(&msg.MessageID, &msg.Status, &msg.Data)
	if err != nil {
		return nil, err
	}
	return msg, nil
}

func (lq *PQueue) pump() error {
	go func() {
		for nmsgs := range lq.notify {
			atomic.AddInt64(&lq.queuelen, nmsgs)
		}
	}()
	go func() {
		for {
			if lq.queuelen > 0 {
				nm, err := lq.next()
				// Keep Message Pump going, probably should read more than one from storage
				if err != nil {
					log.Printf("pump goroutine failed during lq.Next with err: %v", err)
					time.Sleep(30 * time.Second)
					continue
				}
				lq.internal <- nm
			} else {
				//ugly busy loop, but decoupled from Put
				time.Sleep(1 * time.Second)
			}
		}
	}()
	return nil
}

// Subscribe function registers a callback function
// to be called after the db is initially read
// or a new message is put into the queue.
//
// Only one callback function can be registered,
// if a second function is registered, an error is returned.
func (lq *PQueue) Subscribe(cb func(*PMessage) error) error {
	if lq.subscribed {
		return errors.New("already subscribed cannot have multiple subs")
	}

	lq.subscribed = true

	go func() {
		for msg := range lq.internal {
			err := cb(msg)
			if err != nil {
				ierr := lq.MarkFailed(msg.MessageID)
				if ierr != nil {
					fmt.Println("error during marking", ierr)
				}
			} else {
				ierr := lq.Done(msg.MessageID)
				if ierr != nil {
					fmt.Println("error during done", ierr)
				}
			}
		}
	}()

	return nil
}

// Put adds a new message to the end
// of the queue, put will not block.
//
// Put will return an error if the queue
// is full
func (lq *PQueue) Put(data []byte) error {
	if lq.queuelen == lq.maxsize {
		return errors.New("queue is full, message can not be added")
	}
	row := lq.insert.QueryRow(READY, data)
	if row.Err() != nil {
		return row.Err()
	}

	msg := &PMessage{}
	err := row.Scan(&msg.MessageID, &msg.Status, &msg.Data)
	if err != nil {
		return err
	}
	//we dont want to block Put at all, the DB is the buffer
	lq.notify <- 1
	return nil
}

// Peek returnes the oldest element in the list which is in ready state,
// it returns sql.ErrorNoRows if no element is in ready state.
func (lq *PQueue) Peek() (*PMessage, error) {
	row := lq.selnext.QueryRow(READY)
	if row.Err() != nil {
		return nil, row.Err()
	}
	
	msg := &PMessage{}
	err := row.Scan(&msg.MessageID, &msg.Status, &msg.Data)
	if err != nil {
		return nil, err 
	}
	
	return msg, nil
}

// Get returns the element with the given message ID,
// it returns an sql.ErrorNoRows if the element could not be find.
func (lq *PQueue) Get(messageID int64) (*PMessage, error) {
	row := lq.selmsg.QueryRow(messageID)
	if row.Err() != nil {
		return nil, row.Err()
	}

	msg := &PMessage{}
	err := row.Scan(&msg.MessageID, &msg.Status, &msg.Data)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

// Done deletes physically the message from the queue,
// it returns an sql.ErrorNoRows if the element could not be find.
func (lq *PQueue) Done(messageID int64) error {
	res, err := lq.delete.Exec(messageID)
	if err != nil {
		return err
	}
	affectedRows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affectedRows == 0 {
		return sql.ErrNoRows
	}

	atomic.AddInt64(&lq.queuelen, -1)
	return nil
}

// MarkFailed sets the message for given ID to FAILED status,
// which causes the message to stay in the databse, but
// will not be delivered again until it is actively set to retry,
// it returns an sql.ErrorNoRows if the element could not be find.
func (lq *PQueue) MarkFailed(messageID int64) error {
	res, err := lq.update.Exec(FAILED, messageID)
	if err != nil {
		return err
	}
	affectedRows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affectedRows == 0 {
		return sql.ErrNoRows
	}

	atomic.AddInt64(&lq.queuelen, -1)
	return nil
}

// Retry sets the message for given ID to READY status,
// which causes the message to be delivered again
// until it is actively set to retry,
// it returns an sql.ErrorNoRows if the element could not be find.
func (lq *PQueue) Retry(messageID int64) error {
	res, err := lq.update.Exec(READY, messageID)
	if err != nil {
		return err
	}
	affectedRows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if affectedRows == 0 {
		return sql.ErrNoRows
	}

	atomic.AddInt64(&lq.queuelen, 1)
	return nil
}

// RetryAll sets all messages from FAILED to READY state.
func (lq *PQueue) RetryAll() error {
	res, err := lq.updateall.Exec()
	if err != nil {
		return err
	}
	affectedRows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	atomic.AddInt64(&lq.queuelen, affectedRows)
	return nil
}

// Size returns the number of elements in the
// persistent queue where the state is READY.
func (lq *PQueue) Size() (int64, error) {
	row := lq.sizeStmt.QueryRow(FAILED)
	if row.Err() != nil {
		return -1, row.Err()
	}
	count := int64(0)
	err := row.Scan(&count)
	if err != nil {
		return -1, err
	}
	return count, nil
}

// IsEmpty returns true if the queue has 0 READY messages.
func (lq *PQueue) IsEmpty() (bool, error) {
	count, err := lq.Size()
	if err != nil {
		return false, err
	}
	return count == 0, nil
}

// IsFull returns true if the queues size is equal to max size.
func (lq *PQueue) IsFull() (bool, error) {
	count, err := lq.Size()
	if err != nil {
		return false, err
	}
	return count >= lq.maxsize, nil
}

// Close closes internal channels
// and the database connection itself
//
// reuse requires to create a new PQueue
func (lq *PQueue) Close() error {
	close(lq.notify)
	return lq.conn.Close()
}
