package squeuelite

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type PQueue struct {
	conn           *sql.DB
	maxsize        int64
	selmsg string
	selnext string
	insert string
}

func NewPQueue(connStr string, maxsize int64) (*PQueue, error) {
	// file:test.db?cache=shared&mode=memory&_auto_vacuum=2&_journal_mode=wal
	conn, err := sql.Open("sqlite3", connStr)
	if err != nil {
		return nil, err
	}

	lq := &PQueue{
		conn:           conn,
		maxsize:        maxsize,
		selmsg:		"select message_id, status, data from queue where message_id = ?",
		selnext:	"select message_id, status, data from queue where status = ? order by message_id asc limit 1",
		insert:		"insert into queue( status, data, in_time ) VALUES ( ?, ? , unixepoch())",
	}

	if err = lq.init(); err != nil {
		return nil, err
	}

	return lq, nil
}

func (lq *PQueue) init() error {
	tx, err := lq.conn.Begin()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			err = tx.Commit()
		}
	}()

	_, err = lq.conn.Exec(`
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

	//if _, err = lq.conn.Exec("CREATE INDEX IF NOT EXISTS SIdx ON Queue(Status)", []driver.Value{}); err != nil {
	//	return err
	//}

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

func (lq *PQueue) Put(data []byte) error {
	_ , err := lq.conn.Exec(lq.insert, READY, data)
	if err != nil {
		return err
	}

	return nil
}

func (lq *PQueue) Peek() (*PMessage, error) {
	row := lq.conn.QueryRow(lq.selnext, READY )
	if row.Err() != nil {
		return nil, row.Err()
	}
	msg := &PMessage{}
	row.Scan(&msg.MessageID, &msg.Status, &msg.Data)
	return msg, nil
}

/*
func (lq *PQueue) PeekBlock() (*PMessage, error) {
	row, err := lq.conn.Query(lq.selnext, READY)
	if err != nil {
		return nil, err
	}
	defer row.Close()

	message := PMessage{}
	vals := []driver.Value{message.Data, message.MessageID, message.Status, message.inTime, message.lockTime, message.doneTime}
	err = row.Next(vals)
	if err != nil {
		if err == io.EOF {
			//explicitly wait for a notification
			if lq.peekTimeoutSec == -1 {
				//no timeout, continue processing
				<-lq.notify
			} else {
				select {
				case <-lq.notify:
					//continue processing
				case <-time.After(time.Duration(int64(lq.peekTimeoutSec) * int64(time.Second))):
					return nil, sql.ErrNoRows
				}
			}

			row2, err := lq.conn.Query("SELECT * FROM Queue WHERE status = ? ORDER BY inTime ASC LIMIT 1", []driver.Value{READY})
			if err != nil {
				return nil, err
			}
			defer row2.Close()
			err = row2.Next(vals)
			if err != nil && err != io.EOF {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	message.Data = vals[0].(string)
	message.MessageID = vals[1].(string)
	message.Status = vals[2].(int64)
	message.inTime = vals[3].(int64)
	message.lockTime = vals[4].(int64)
	message.doneTime = vals[5].(int64)

	return &message, nil
}
*/

func (lq *PQueue) Get(messageID int64) (*PMessage, error) {
	row := lq.conn.QueryRow(lq.selmsg,  messageID)
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
	_, err := lq.conn.Exec(`UPDATE queue SET status = ?, done_time = unixepoch() WHERE message_id = ?`,  DONE, messageID)

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

/*
func (lq *PQueue) ListLocked(threshold_seconds int) ([]Message, error) {
	threshold_nanoseconds := int64(threshold_seconds) * int64(time.Second)

	rows, err := lq.conn.Query(`
        SELECT * FROM Queue
        WHERE
            status = ?
            AND lockTime < ?
    `, []driver.Value{LOCKED, time.Now().UnixNano() - threshold_nanoseconds})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	messages := []Message{}

	for {
		message := Message{}
		vals := []driver.Value{message.Data, message.MessageID, message.Status, message.inTime, message.lockTime, message.doneTime}
		err = rows.Next(vals)
		if err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			break
		}
		message.Data = vals[0].(string)
		message.MessageID = vals[1].(string)
		message.Status = vals[2].(int64)
		message.inTime = vals[3].(int64)
		message.lockTime = vals[4].(int64)
		message.doneTime = vals[5].(int64)
		messages = append(messages, message)
	}

	return messages, nil
}
*/

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
