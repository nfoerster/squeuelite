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

type SQueue struct {
	conn       *sql.DB
	notify     chan int64
	internal   chan *PMessage
	subscribed bool

	insert   *sql.Stmt
	update   *sql.Stmt
	nextfree *sql.Stmt
	delete   *sql.Stmt
}

// SQueue is a shortcut for Subscriber Queue
// which is a persistent queue that
// call a certain callback function
// after a new message is put into the queue
func NewSQueue(connStr string) (*SQueue, error) {
	// file:test.db?cache=shared&mode=memory&_auto_vacuum=2&_journal_mode=wal
	conn, err := sql.Open(fmt.Sprintf("sqlite3%v", ""), connStr)
	if err != nil {
		return nil, err
	}

	lq := &SQueue{
		conn:     conn,
		notify:   make(chan int64, 10),
		internal: make(chan *PMessage, 10),
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

	updateStmt, err := lq.conn.Prepare("UPDATE queue SET status = ?, done_time = unixepoch() WHERE message_id = ?")
	if err != nil {
		return nil, err
	}
	lq.update = updateStmt

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

func (lq *SQueue) init() error {
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

func (lq *SQueue) next() (*PMessage, error) {
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

func (lq *SQueue) pump() error {
	var queueSize int64
	go func() {
		for nmsgs := range lq.notify {
			atomic.AddInt64(&queueSize, nmsgs)
		}
	}()
	go func() {
		for {
			if queueSize > 0 {
				nm, err := lq.next()
				// Keep Message Pump going, probably should read more than one from storage
				if err != nil {
					log.Printf("pump goroutine failed during lq.Next with err: %v", err)
					time.Sleep(30 * time.Second)
					continue
				}
				lq.internal <- nm
				atomic.AddInt64(&queueSize, -1)
			} else {
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
// if a second function is registered, the first is
// replaced
func (lq *SQueue) Subscribe(cb func(*PMessage) error) error {
	if lq.subscribed {
		return errors.New("already subscribed cannot have multiple subs")
	}

	lq.subscribed = true

	go func() {
		for msg := range lq.internal {
			err := cb(msg)
			if err != nil {
				ierr := lq.updateState(msg.MessageID, FAILED)
				if ierr != nil {
					fmt.Println("error during marking", ierr)
				}
				// add to notify channel again to retry?
			} else {
				ierr := lq.done(msg.MessageID)
				if ierr != nil {
					fmt.Println("erro during done", ierr)
				}
			}
		}
	}()

	return nil
}

// Put adds a new message to the end
// of the queue, put will not block
func (lq *SQueue) Put(data []byte) error {
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

func (lq *SQueue) updateState(messageID int64, state int64) error {
	_, err := lq.update.Exec(state, messageID)

	if err != nil {
		return err
	}

	return nil
}

func (lq *SQueue) done(messageID int64) error {
	_, err := lq.delete.Exec(messageID)

	if err != nil {
		return err
	}

	return nil
}

// Close closes internal channels
// and the database connection itself
//
// reuse requires to create a new SQueue
func (lq *SQueue) Close() {
	close(lq.notify)
	lq.conn.Close()
}

type PQueue struct {
	conn       *sql.DB
	internal   chan *PMessage
	subscribed bool
	queuelen   int64
	maxsize    int64

	// sql as struct members feels wrong
	selmsg       *sql.Stmt
	selnext      *sql.Stmt
	selnextbatch *sql.Stmt
	insert       *sql.Stmt
	update       *sql.Stmt
	delete       *sql.Stmt
	nextfree     *sql.Stmt
}

func NewPQueue(connStr string, maxsize int64) (*PQueue, error) {
	// file:test.db?cache=shared&mode=memory&_auto_vacuum=2&_journal_mode=wal
	conn, err := sql.Open("sqlite3", connStr)
	if err != nil {
		return nil, err
	}

	lq := &PQueue{
		conn:     conn,
		maxsize:  maxsize,
		queuelen: 0,
		internal: make(chan *PMessage, 10),
	}

	if err = lq.init(); err != nil {
		return nil, err
	}

	//prepared statements
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

	selnextbatchStmt, err := lq.conn.Prepare("select message_id, status, data from queue where status = ? order by in_time asc limit 10")
	if err != nil {
		return nil, err
	}
	lq.selnextbatch = selnextbatchStmt

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

	deleteStmt, err := lq.conn.Prepare("DELETE FROM queue WHERE message_id = ?")
	if err != nil {
		return nil, err
	}
	lq.delete = deleteStmt

	nextfreeStmt, err := lq.conn.Prepare("UPDATE queue SET status = ? where message_id = (Select message_id from queue where status = ? limit 1) returning message_id, status, data")
	if err != nil {
		return nil, err
	}
	lq.nextfree = nextfreeStmt

	// if starting from populated queue we are starting with capacity
	row := conn.QueryRow("select count(*) from queue")
	if row.Err() != nil {
		return nil, row.Err()
	}

	row.Scan(&lq.queuelen)

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

func (lq *PQueue) Next() (*PMessage, error) {
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

// Keep Message Pump going, probably should read more than one from storage
func (lq *PQueue) Pump() error {
	if lq.queuelen > 0 {
		nm, err := lq.Next()
		if err != nil {
			return err
		}
		lq.internal <- nm
	}
	return nil
}

func (lq *PQueue) Subscribe(cb func(*PMessage) error) error {
	if lq.subscribed {
		return errors.New("already subscribed cannot have multiple subs")
	}

	lq.subscribed = true

	go func() {
		lq.Pump()
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
					fmt.Println("erro during done", ierr)
				}
				atomic.AddInt64(&lq.queuelen, -1)
			}
			err = lq.Pump()
			if err != nil {
				fmt.Println("error during pump", err)
			}
		}
	}()

	return nil
}

func (lq *PQueue) Put(data []byte) error {
	row := lq.insert.QueryRow(READY, data)
	if row.Err() != nil {
		return row.Err()
	}

	msg := &PMessage{}
	err := row.Scan(&msg.MessageID, &msg.Status, &msg.Data)
	if err != nil {
		return err
	}

	atomic.AddInt64(&lq.queuelen, 1)
	return nil
}

func (lq *PQueue) Peek() (*PMessage, error) {
	row := lq.selnext.QueryRow(READY)
	if row.Err() != nil {
		return nil, row.Err()
	}
	msg := &PMessage{}
	row.Scan(&msg.MessageID, &msg.Status, &msg.Data)
	return msg, nil
}

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

func (lq *PQueue) Done(messageID int64) error {
	_, err := lq.delete.Exec(messageID)

	if err != nil {
		return err
	}

	return nil
}

func (lq *PQueue) MarkFailed(messageID int64) error {
	_, err := lq.update.Exec(FAILED, messageID)

	if err != nil {
		return err
	}

	return nil
}
func (lq *PQueue) Retry(messageID string) error {
	_, err := lq.update.Exec(READY, messageID)

	if err != nil {
		return err
	}
	return nil
}

func (lq *PQueue) Qsize() (int64, error) {
	rows := lq.conn.QueryRow(`SELECT COUNT(*) FROM Queue WHERE status <> ?`, FAILED)
	if rows.Err() != nil {
		return -1, rows.Err()
	}
	count := int64(0)
	err := rows.Scan(&count)
	if err != nil {
		return -1, err
	}
	return count, nil
}

func (lq *PQueue) Empty() (bool, error) {
	count, err := lq.Qsize()
	if err != nil {
		return false, err
	}
	return count == 0, nil

}

func (lq *PQueue) Full() (bool, error) {
	count, err := lq.Qsize()
	if err != nil {
		return false, err
	}
	return count >= lq.maxsize, nil
}

func (lq *PQueue) Close() {
	lq.conn.Close()
}
