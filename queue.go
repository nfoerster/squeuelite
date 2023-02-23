package squeuelite

import (
	"database/sql"
	"errors"
	"fmt"
	"sync/atomic"

	_ "github.com/mattn/go-sqlite3"
)

type SQueue struct {
	conn     *sql.DB
	notify chan int
	internal chan *PMessage
	subscribed bool 

	insert   string
	update string
	nextfree string
}


func NewSQueue(connStr string) (*SQueue, error) {
	// file:test.db?cache=shared&mode=memory&_auto_vacuum=2&_journal_mode=wal
	conn, err := sql.Open("sqlite3", connStr)
	if err != nil {
		return nil, err
	}
	
	lq := &SQueue{
		conn:     conn,
		notify: make(chan int, 10),
		internal: make(chan *PMessage, 10),
		insert:   "insert into queue( status, data, in_time ) VALUES ( ?, ? , unixepoch()) returning message_id, status, data",
		update: "UPDATE queue SET status = ?, done_time = unixepoch() WHERE message_id = ?",
		nextfree: "UPDATE queue SET status = ? where message_id = (Select message_id from queue where status = ? order by message_id asc limit 1) returning message_id, status, data",
	}

	if err = lq.init(); err != nil {
		return nil, err
	}

	// if starting from populated queue we are starting with capacity in notify
	row := conn.QueryRow("select count(*) from queue where status <> ?", DONE)
        if row.Err() != nil {
		return nil, row.Err() 
	}
	queuelen := 0
	row.Scan(&queuelen)
	lq.notify <- queuelen
	//start notification
	go lq.Pump()
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


func (lq *SQueue) Next() (*PMessage, error) {
	row := lq.conn.QueryRow(lq.nextfree, LOCKED, READY)
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

func (lq *SQueue) Pump() error {
	for nmsgs := range lq.notify {
		for ii := 0; ii < nmsgs; ii++ {
			nm, err := lq.Next()
			// Keep Message Pump going, probably should read more than one from storage
			if err != nil {
				return err
			}
			lq.internal <- nm
		}
	}
	return nil
}

func (lq *SQueue) Subscribe(cb func(*PMessage) error) error {
        if lq.subscribed {
		return errors.New("already subscribed cannot have multiple subs")
	}

	lq.subscribed = true

	go func() {
		for msg := range lq.internal {
			err := cb(msg)
			if err != nil {
				ierr := lq.StatusChange(msg.MessageID, FAILED)
				if ierr != nil {
				     fmt.Println("error during marking", ierr)
				}
				// add to notify channel again to retry?
			} else {
				ierr := lq.StatusChange(msg.MessageID, DONE)
				if ierr != nil {
					fmt.Println("erro during done", ierr)
				}
			}
		}
	}()

	return nil
}

func (lq *SQueue) Put(data []byte) error {
	row := lq.conn.QueryRow(lq.insert, READY, data)
	if row.Err() != nil {
		return row.Err()
	}

	msg := &PMessage{}
	err := row.Scan(&msg.MessageID, &msg.Status, &msg.Data)
	if err != nil {
		return err
	}
	lq.notify <- 1
	return nil
}


func (lq *SQueue) StatusChange(messageID int64, state int64) error {
	_, err := lq.conn.Exec(lq.update, state, messageID)

	if err != nil {
		return err
	}

	return nil
}

func (lq *SQueue) Close() {
	close(lq.notify)
}

type PQueue struct {
	conn     *sql.DB
	internal chan *PMessage
	subscribed bool 
	queuelen int64
	maxsize  int64

	// sql as struct members feels wrong
	selmsg   string
	selnext  string
	selnextbatch string
	insert   string
	update string
	nextfree string
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
		selmsg:   "select message_id, status, data from queue where message_id = ?",
		selnext:  "select message_id, status, data from queue where status = ? order by message_id asc limit 1",
		selnextbatch:  "select message_id, status, data from queue where status = ? order by message_id asc limit 10",
		insert:   "insert into queue( status, data, in_time ) VALUES ( ?, ? , unixepoch()) returning message_id, status, data",
		update: "UPDATE queue SET status = ?, done_time = unixepoch() WHERE message_id = ?",
		nextfree: "UPDATE queue SET status = ? where message_id = (Select message_id from queue where status = ? limit 1) returning message_id, status, data",
	}

	if err = lq.init(); err != nil {
		return nil, err
	}

	// if starting from populated queue we are starting with capacity
	row := conn.QueryRow("select count(*) from queue where status <> ?", DONE)
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

	// would need an index for this but we expect that we do not have thousands of records here
	if lq.maxsize > 0 {
		if _, err = lq.conn.Exec(fmt.Sprintf(`
            CREATE TRIGGER IF NOT EXISTS maxsize_control
            BEFORE INSERT ON Queue
            WHEN (SELECT COUNT(*) FROM queue WHERE status = %d) >= %d
            BEGIN
                SELECT RAISE(ABORT, 'Max queue length reached: %d');
            END;`,
			READY, lq.maxsize, lq.maxsize)); err != nil {
			return err
		}
	}

	return nil
}

func (lq *PQueue) Next() (*PMessage, error) {
	row := lq.conn.QueryRow(lq.nextfree, LOCKED, READY)
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
				atomic.AddInt64(&lq.queuelen,-1)
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
	row := lq.conn.QueryRow(lq.insert, READY, data)
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
	row := lq.conn.QueryRow(lq.selnext, READY)
	if row.Err() != nil {
		return nil, row.Err()
	}
	msg := &PMessage{}
	row.Scan(&msg.MessageID, &msg.Status, &msg.Data)
	return msg, nil
}

func (lq *PQueue) Get(messageID int64) (*PMessage, error) {
	row := lq.conn.QueryRow(lq.selmsg, messageID)
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
	_, err := lq.conn.Exec(lq.update, DONE, messageID)

	if err != nil {
		return err
	}

	return nil
}

func (lq *PQueue) MarkFailed(messageID int64) error {
	_, err := lq.conn.Exec(`
        UPDATE Queue SET
            status = ?
            , doneTime = CURRENT_TIMESTAMP
        WHERE messageID = ?
    `, FAILED, messageID)

	if err != nil {
		return err
	}

	return nil
}
func (lq *PQueue) Retry(messageID string) error {
	_, err := lq.conn.Exec(`
        UPDATE Queue SET
            status = ?
            , doneTime = CURRENT_TIMESTAMP
        WHERE messageID = ?
    `, READY, messageID)

	if err != nil {
		return err
	}
	return nil
}

func (lq *PQueue) Qsize() (int64, error) {
	rows := lq.conn.QueryRow(`SELECT COUNT(*) FROM Queue WHERE status NOT IN (?, ?)`, DONE, FAILED)
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

func (lq *PQueue) Prune() error {
	_, err := lq.conn.Exec("DELETE FROM Queue WHERE status IN (?, ?)", DONE, FAILED)
	if err != nil {
		return err
	}
	return nil
}

func (lq *PQueue) Close() error {
	return lq.conn.Close()
}

/*
func (lq *PQueue) RetryFailed() error {
	result, err := lq.conn.Exec(`
        UPDATE Queue SET
            status = ?
            , doneTime = 0
        WHERE status = ?
    `, READY, FAILED)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	var i int64
	for i = 0; i < rowsAffected; i++ {
		go func(i int64) {
			lq.notify <- i
		}(i)
	}

	return nil
}
*/
/*
func (lq *SQueueLite) Transaction(mode string) (func(), error) {
	if mode != "DEFERRED" && mode != "IMMEDIATE" && mode != "EXCLUSIVE" {
		return nil, fmt.Errorf("Transaction mode '%s' is not valid", mode)
	}
	_, err := lq.conn.Exec(fmt.Sprintf("BEGIN %s", mode), []driver.Value{})
	if err != nil {
		return nil, err
	}
	return func() {
		if err != nil {
			lq.conn.Exec("ROLLBACK", []driver.Value{})
		} else {
			lq.conn.Exec("COMMIT", []driver.Value{})
		}
	}, nil
}

func (lq *SQueueLite) String() string {
	rows, err := lq.conn.Query("SELECT * FROM Queue LIMIT 3", []driver.Value{})
	if err != nil {
		return fmt.Sprintf("%T(Connection=%v)", lq, lq.conn)
	}
	defer rows.Close()
	displayItems := []Message{}

	for {
		message := Message{}
		vals := []driver.Value{message.Data, message.MessageID, message.Status, message.inTime, message.lockTime, message.doneTime}
		err = rows.Next(vals)
		if err != nil && err != io.EOF {
			return fmt.Sprintf("%T(Connection=%v)", lq, lq.conn)
		} else if err == io.EOF {
			break
		}
		message.Data = vals[0].(string)
		message.MessageID = vals[1].(string)
		message.Status = vals[2].(int64)
		message.inTime = vals[3].(int64)
		message.lockTime = vals[4].(int64)
		message.doneTime = vals[5].(int64)
		displayItems = append(displayItems, message)
	}
	return fmt.Sprintf("%T(Connection=%v, items=%v)", lq, lq.conn, displayItems)
}
*/
