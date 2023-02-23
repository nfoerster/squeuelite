package squeuelite

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	sqlite3 "github.com/mattn/go-sqlite3"
)

type SQueueLite struct {
	conn           *sqlite3.SQLiteConn
	maxsize        int
	notify         chan int64
	peekTimeoutSec int
}

func NewSQueueLite(filenameOrConn string, memory bool, maxsize int) (*SQueueLite, error) {
	return NewSQueueLiteWithTimeout(filenameOrConn, memory, maxsize, -1)
}

func NewSQueueLiteWithTimeout(filenameOrConn string, memory bool, maxsize int, peekTimeoutSec int) (*SQueueLite, error) {
	if filenameOrConn == "" && !memory {
		return nil, errors.New("either specify a filenameOrConn or pass memory=true")
	}

	notifyChan := make(chan int64, 1)

	conn, err := sql.Open("sqlite3", buildDSN(filenameOrConn, memory))
	if err != nil {
		return nil, err
	}
	driverConn, err := conn.Driver().Open(buildDSN(filenameOrConn, memory))
	if err != nil {
		return nil, err
	}
	sqliteConn, ok := driverConn.(*sqlite3.SQLiteConn)
	if !ok {
		return nil, errors.New("internal driver error")
	}

	lq := &SQueueLite{
		conn:           nil,
		maxsize:        maxsize,
		notify:         notifyChan,
		peekTimeoutSec: peekTimeoutSec,
	}
	sqliteConn.RegisterUpdateHook(lq.updateFunc)

	lq.conn = sqliteConn

	if err = lq.init(); err != nil {
		return nil, err
	}

	return lq, nil
}

func buildDSN(filenameOrConn string, memory bool) string {
	if memory {
		return ":memory:"
	}

	if filepath.Ext(filenameOrConn) != ".db" {
		filenameOrConn += ".db"
	}

	return filenameOrConn
}

func (lq *SQueueLite) updateFunc(mode int, database, table string, rowid int64) {
	switch mode {
	case sqlite3.SQLITE_INSERT:
		go func() {
			lq.notify <- rowid
		}()
	}
}

func (lq *SQueueLite) init() error {
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

	if _, err = lq.conn.Exec(`
        CREATE TABLE IF NOT EXISTS Queue
        (
          data      TEXT    NOT NULL,
          messageID TEXT    NOT NULL,
          status    INTEGER NOT NULL,
          inTime    INTEGER NOT NULL,
          lockTime  INTEGER NOT NULL,
          doneTime  INTEGER NOT NULL,
          PRIMARY KEY (MessageID)
        )`, []driver.Value{}); err != nil {
		return err
	}

	if _, err = lq.conn.Exec("CREATE INDEX IF NOT EXISTS SIdx ON Queue(Status)", []driver.Value{}); err != nil {
		return err
	}

	if lq.maxsize > 0 {
		if _, err = lq.conn.Exec(fmt.Sprintf(`
            CREATE TRIGGER IF NOT EXISTS maxsize_control
            BEFORE INSERT ON Queue
            WHEN (SELECT COUNT(*) FROM Queue WHERE Status = %d) >= %d
            BEGIN
                SELECT RAISE(ABORT, 'Max queue length reached: %d');
            END;`,
			READY, lq.maxsize, lq.maxsize), []driver.Value{}); err != nil {
			return err
		}
	}

	return nil
}

func (lq *SQueueLite) Put(data []byte) error {
	uuid, err := uuid.NewUUID()
	if err != nil {
		return err
	}
	messageID := uuid.String()
	now := time.Now().UnixNano()
	_, err = lq.conn.Exec(`
			INSERT INTO
			  Queue(  data,  messageID, status, inTime, lockTime, doneTime )
			VALUES ( ?, ?, ?, ?, 0, 0 )
			`, []driver.Value{string(data), messageID, READY, now},
	)
	if err != nil {
		return err
	}

	return nil
}

func (lq *SQueueLite) Peek() (*Message, error) {
	row, err := lq.conn.Query("SELECT * FROM Queue WHERE status = ? ORDER BY inTime ASC LIMIT 1", []driver.Value{READY})
	if err != nil {
		return nil, err
	}
	defer row.Close()
	message := Message{}
	vals := []driver.Value{message.Data, message.MessageID, message.status, message.inTime, message.lockTime, message.doneTime}
	err = row.Next(vals)
	if err != nil && err != io.EOF {
		return nil, err
	} else if err == io.EOF {
		return nil, sql.ErrNoRows
	}
	message.Data = []byte(vals[0].(string))
	message.MessageID = vals[1].(string)
	message.status = vals[2].(int64)
	message.inTime = vals[3].(int64)
	message.lockTime = vals[4].(int64)
	message.doneTime = vals[5].(int64)

	//erase notify if available
	select {
	case <-lq.notify:
	default:
	}

	return &message, nil
}

func (lq *SQueueLite) PeekBlock() (*Message, error) {
	row, err := lq.conn.Query("SELECT * FROM Queue WHERE status = ? ORDER BY inTime ASC LIMIT 1", []driver.Value{READY})
	if err != nil {
		return nil, err
	}
	defer row.Close()

	message := Message{}
	vals := []driver.Value{string(message.Data), message.MessageID, message.status, message.inTime, message.lockTime, message.doneTime}
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
	message.Data = []byte(vals[0].(string))
	message.MessageID = vals[1].(string)
	message.status = vals[2].(int64)
	message.inTime = vals[3].(int64)
	message.lockTime = vals[4].(int64)
	message.doneTime = vals[5].(int64)

	return &message, nil
}

func (lq *SQueueLite) Get(messageID string) (*Message, error) {
	row, err := lq.conn.Query("SELECT * FROM Queue WHERE messageID = ?", []driver.Value{messageID})
	if err != nil {
		return nil, err
	}
	defer row.Close()

	message := Message{}
	vals := []driver.Value{message.Data, message.MessageID, message.status, message.inTime, message.lockTime, message.doneTime}
	err = row.Next(vals)
	if err != nil && err != io.EOF {
		return nil, err
	}
	message.Data = []byte(vals[0].(string))
	message.MessageID = vals[1].(string)
	message.status = vals[2].(int64)
	message.inTime = vals[3].(int64)
	message.lockTime = vals[4].(int64)
	message.doneTime = vals[5].(int64)

	return &message, nil
}

func (lq *SQueueLite) Done(messageID string) error {
	now := time.Now().UnixNano()

	_, err := lq.conn.Exec(`
        UPDATE Queue SET
            status = ?
            , doneTime = ?
        WHERE messageID = ?
    `, []driver.Value{DONE, now, messageID})

	if err != nil {
		return err
	}

	return nil
}

func (lq *SQueueLite) MarkFailed(messageID string) error {
	now := time.Now().UnixNano()

	_, err := lq.conn.Exec(`
        UPDATE Queue SET
            status = ?
            , doneTime = ?
        WHERE messageID = ?
    `, []driver.Value{FAILED, now, messageID})

	if err != nil {
		return err
	}

	return nil
}

func (lq *SQueueLite) ListLocked(threshold_seconds int) ([]Message, error) {
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
		vals := []driver.Value{message.Data, message.MessageID, message.status, message.inTime, message.lockTime, message.doneTime}
		err = rows.Next(vals)
		if err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			break
		}
		message.Data = []byte(vals[0].(string))
		message.MessageID = vals[1].(string)
		message.status = vals[2].(int64)
		message.inTime = vals[3].(int64)
		message.lockTime = vals[4].(int64)
		message.doneTime = vals[5].(int64)
		messages = append(messages, message)
	}

	return messages, nil
}

func (lq *SQueueLite) Retry(messageID string) error {
	_, err := lq.conn.Exec(`
        UPDATE Queue SET
            status = ?
            , doneTime = 0
        WHERE messageID = ?
    `, []driver.Value{READY, messageID})

	if err != nil {
		return err
	}
	return nil
}

func (lq *SQueueLite) RetryFailed() error {
	result, err := lq.conn.Exec(`
        UPDATE Queue SET
            status = ?
            , doneTime = 0
        WHERE status = ?
    `, []driver.Value{READY, FAILED})
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

func (lq *SQueueLite) Qsize() (int, error) {
	var count int
	rows, err := lq.conn.Query(`
        SELECT COUNT(*) FROM Queue
        WHERE status NOT IN (?, ?)
    `, []driver.Value{DONE, FAILED})
	if err != nil {
		return -1, err
	}
	defer rows.Close()
	vals := []driver.Value{count}
	err = rows.Next(vals)
	if err != nil {
		return -1, err
	}
	count = int(vals[0].(int64))
	return count, nil
}

func (lq *SQueueLite) Empty() (bool, error) {
	var count int
	rows, err := lq.conn.Query(`
        SELECT COUNT(*) as cnt FROM Queue WHERE status = ?
    `, []driver.Value{READY})
	if err != nil {
		return false, err
	}
	defer rows.Close()
	vals := []driver.Value{count}
	err = rows.Next(vals)
	if err != nil {
		return false, err
	}
	count = int(vals[0].(int64))
	return count == 0, nil
}

func (lq *SQueueLite) Full() (bool, error) {
	var count int
	rows, err := lq.conn.Query(`
        SELECT COUNT(*) as cnt FROM Queue WHERE status = ?
    `, []driver.Value{READY})
	if err != nil {
		return false, err
	}
	defer rows.Close()
	vals := []driver.Value{count}
	err = rows.Next(vals)
	if err != nil {
		return false, err
	}
	count = int(vals[0].(int64))
	return count >= lq.maxsize, nil
}

func (lq *SQueueLite) Prune() error {
	_, err := lq.conn.Exec("DELETE FROM Queue WHERE status IN (?, ?)", []driver.Value{DONE, FAILED})
	if err != nil {
		return err
	}
	return nil
}

func (lq *SQueueLite) Vacuum() error {
	_, err := lq.conn.Exec("VACUUM", []driver.Value{})
	return err
}

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
		vals := []driver.Value{message.Data, message.MessageID, message.status, message.inTime, message.lockTime, message.doneTime}
		err = rows.Next(vals)
		if err != nil && err != io.EOF {
			return fmt.Sprintf("%T(Connection=%v)", lq, lq.conn)
		} else if err == io.EOF {
			break
		}
		message.Data = []byte(vals[0].(string))
		message.MessageID = vals[1].(string)
		message.status = vals[2].(int64)
		message.inTime = vals[3].(int64)
		message.lockTime = vals[4].(int64)
		message.doneTime = vals[5].(int64)
		displayItems = append(displayItems, message)
	}
	return fmt.Sprintf("%T(Connection=%v, items=%v)", lq, lq.conn, displayItems)
}

func (lq *SQueueLite) Close() error {
	return lq.conn.Close()
}
