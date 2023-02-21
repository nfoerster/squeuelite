package squeuelite

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	sqlite3 "github.com/mattn/go-sqlite3"
)

type MessageStatus int

const (
	READY MessageStatus = iota
	LOCKED
	DONE
	FAILED
)

type Message struct {
	Data      string
	MessageID string
	Status    MessageStatus
	inTime    int64
	lockTime  *int64
	doneTime  *int64
}

type SQueueLite struct {
	conn    *sql.DB
	maxsize int
	notify  chan int
}

func NewSQueueLite(filenameOrConn string, memory bool, maxsize int) (*SQueueLite, error) {
	if filenameOrConn == "" && !memory {
		return nil, errors.New("either specify a filenameOrConn or pass memory=true")
	}

	notifyChan := make(chan int, 1)

	lq := &SQueueLite{
		conn:    nil,
		maxsize: maxsize,
		notify:  notifyChan,
	}

	registerHook(lq.updateFunc)

	conn, err := sql.Open("sqlite3_with_update_hook", buildDSN(filenameOrConn, memory))
	if err != nil {
		return nil, err
	}

	lq.conn = conn

	if err = lq.init(); err != nil {
		return nil, err
	}

	return lq, nil
}

func registerHook(callback func(mode int, database, table string, rowid int64)) {
	sql.Register("sqlite3_with_update_hook",
		&sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				conn.RegisterUpdateHook(callback)
				return nil
			},
		})
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
	//log.Printf("%+v,%+v,%+v,%+v", mode, database, table, rowid)
	switch mode {
	case sqlite3.SQLITE_INSERT:
		// 	fallthrough
		// case sqlite3.SQLITE_UPDATE:
		go func() {
			lq.notify <- mode
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

	if _, err = tx.Exec(`
        CREATE TABLE IF NOT EXISTS Queue
        (
          Data      TEXT    NOT NULL,
          MessageID TEXT    NOT NULL,
          Status    INTEGER NOT NULL,
          inTime    INTEGER NOT NULL,
          lockTime  INTEGER,
          doneTime  INTEGER,
          PRIMARY KEY (MessageID)
        )`); err != nil {
		return err
	}

	if _, err = tx.Exec("CREATE INDEX IF NOT EXISTS SIdx ON Queue(Status)"); err != nil {
		return err
	}

	if lq.maxsize > 0 {
		if _, err = tx.Exec(fmt.Sprintf(`
            CREATE TRIGGER IF NOT EXISTS maxsize_control
            BEFORE INSERT ON Queue
            WHEN (SELECT COUNT(*) FROM Queue WHERE Status = %d) >= %d
            BEGIN
                SELECT RAISE(ABORT, 'Max queue length reached: %d');
            END;`,
			READY, lq.maxsize, lq.maxsize)); err != nil {
			return err
		}
	}

	return nil
}

func (lq *SQueueLite) Put(Data string) (*Message, error) {
	uuid, err := uuid.NewUUID()
	if err != nil {
		return nil, err
	}
	MessageID := uuid.String()
	now := time.Now().UnixNano()
	_, err = lq.conn.Exec(`
			INSERT INTO
			  Queue(  Data,  MessageID, Status,                 inTime, lockTime, doneTime )
			VALUES ( ?, ?, ?, ?, NULL, NULL )
			`, Data, MessageID, READY, now,
	)
	if err != nil {
		return nil, err
	}

	return &Message{
		Data,
		MessageID,
		READY,
		now,
		nil,
		nil,
	}, nil
}

// func (lq *SQueueLite) _pop_returning() (*Message, error) {
// 	//with self.transaction(mode="IMMEDIATE"):
// 	row := lq.conn.QueryRow(
// 		`UPDATE
//             Queue
//         SET
//             Status = ?, lockTime = ?
//         WHERE
//             rowid = (SELECT rowid FROM Queue WHERE Status = ? ORDER BY MessageID LIMIT 1) RETURNING *`,
// 		LOCKED, time.Now().UnixNano(), READY,
// 	)
// 	message := Message{}
// 	err := row.Scan(&message.Data, &message.MessageID, &message.Status, &message.inTime, &message.lockTime, &message.doneTime)
// 	if err != nil {
// 		return nil, err
// 	}
// 	message.Status = LOCKED
// 	message.lockTime = ptrInt64(time.Now().UnixNano())

// 	return &message, nil
// }

// func ptrInt64(u int64) *int64 {
// 	return &u
// }

// func (lq *SQueueLite) _pop_transaction() (*Message, error) {
// 	//with self.transaction(mode="IMMEDIATE"):
// 	row := lq.conn.QueryRow(
// 		`SELECT
//             *
//         FROM
//             Queue
//         WHERE
//             rowid = (SELECT rowid FROM Queue WHERE Status = ? ORDER BY MessageID LIMIT 1)`, READY,
// 	)
// 	message := Message{}
// 	err := row.Scan(&message.Data, &message.MessageID, &message.Status, &message.inTime, &message.lockTime, &message.doneTime)
// 	if err != nil {
// 		return nil, err
// 	}
// 	lockTime := time.Now().UnixNano()

// 	_, err = lq.conn.Exec(`
// 			UPDATE Queue SET
// 			Status = ?
// 			, lockTime = ?
// 			WHERE MessageID = ? AND Status = ?`,
// 		LOCKED, lockTime, message.MessageID, READY,
// 	)
// 	if err != nil {
// 		return nil, err
// 	}
// 	message.Status = LOCKED
// 	message.lockTime = ptrInt64(time.Now().UnixNano())

// 	return &message, nil
// }

func (lq *SQueueLite) Peek() (*Message, error) {
	row := lq.conn.QueryRow("SELECT * FROM Queue WHERE Status = ? ORDER BY inTime ASC LIMIT 1", READY)

	message := Message{}
	err := row.Scan(&message.Data, &message.MessageID, &message.Status, &message.inTime, &message.lockTime, &message.doneTime)
	if err != nil {
		return nil, err
	}
	message.Status = READY
	message.lockTime = nil
	message.doneTime = nil

	return &message, nil
}

func (lq *SQueueLite) PeekBlock() (*Message, error) {
	row, err := lq.conn.Query("SELECT * FROM Queue WHERE Status = ? ORDER BY inTime ASC LIMIT 1", READY)
	if err != nil {
		return nil, err
	}
	for !row.Next() {
		<-lq.notify
		row, err = lq.conn.Query("SELECT * FROM Queue WHERE Status = ? ORDER BY inTime ASC LIMIT 1", READY)
		if err != nil {
			return nil, err
		}
	}

	message := Message{}
	err = row.Scan(&message.Data, &message.MessageID, &message.Status, &message.inTime, &message.lockTime, &message.doneTime)
	if err != nil {
		return nil, err
	}
	message.Status = READY
	message.lockTime = nil
	message.doneTime = nil

	return &message, nil
}

func (lq *SQueueLite) Get(MessageID string) (*Message, error) {
	row := lq.conn.QueryRow("SELECT * FROM Queue WHERE MessageID = ?", MessageID)

	message := Message{}
	err := row.Scan(&message.Data, &message.MessageID, &message.Status, &message.inTime, &message.lockTime, &message.doneTime)
	if err != nil {
		return nil, err
	}
	message.Status = READY
	message.lockTime = nil
	message.doneTime = nil

	return &message, nil
}

func (lq *SQueueLite) Done(MessageID string) (int, error) {
	now := time.Now().UnixNano()

	result, err := lq.conn.Exec(`
        UPDATE Queue SET
            Status = ?
            , doneTime = ?
        WHERE MessageID = ?
    `, DONE, now, MessageID)

	if err != nil {
		return -1, err
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		return -1, err
	}

	return int(lastInsertID), nil
}

func (lq *SQueueLite) MarkFailed(MessageID int) (int, error) {
	now := time.Now().UnixNano()

	result, err := lq.conn.Exec(`
        UPDATE Queue SET
            Status = ?
            , doneTime = ?
        WHERE MessageID = ?
    `, FAILED, now, MessageID)

	if err != nil {
		return -1, err
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		return -1, err
	}

	return int(lastInsertID), nil
}

func (lq *SQueueLite) ListLocked(threshold_seconds int) ([]Message, error) {
	threshold_nanoseconds := int64(threshold_seconds) * int64(time.Second)

	rows, err := lq.conn.Query(`
        SELECT * FROM Queue
        WHERE
            Status = ?
            AND lockTime < ?
    `, LOCKED, time.Now().UnixNano()-threshold_nanoseconds)

	if err != nil {
		return nil, err
	}

	messages := []Message{}
	for rows.Next() {
		var message Message
		if err := rows.Scan(&message.Data, &message.MessageID, &message.Status, &message.inTime, &message.lockTime, &message.doneTime); err != nil {
			if err != nil {
				return nil, err
			}
		}
		messages = append(messages, message)
	}

	return messages, nil
}

func (lq *SQueueLite) Retry(MessageID int) (int, error) {
	result, err := lq.conn.Exec(`
        UPDATE Queue SET
            Status = ?
            , doneTime = NULL
        WHERE MessageID = ?
    `, READY, MessageID)

	if err != nil {
		return -1, err
	}

	lastInsertID, err := result.LastInsertId()
	if err != nil {
		return -1, err
	}

	return int(lastInsertID), nil
}

func (lq *SQueueLite) Qsize() (int, error) {
	var count int
	if err := lq.conn.QueryRow(`
        SELECT COUNT(*) FROM Queue
        WHERE Status NOT IN (?, ?)
    `, DONE, FAILED).Scan(&count); err != nil {
		return -1, err
	}
	return count, nil
}

func (lq *SQueueLite) Empty() (bool, error) {
	var cnt int
	if err := lq.conn.QueryRow(`
        SELECT COUNT(*) as cnt FROM Queue WHERE Status = ?
    `, READY).Scan(&cnt); err != nil {
		return false, err
	}
	return cnt == 0, nil
}

func (lq *SQueueLite) Full() (bool, error) {
	row := lq.conn.QueryRow("SELECT COUNT(*) as cnt FROM Queue WHERE Status = ?", READY)
	var cnt int
	err := row.Scan(&cnt)
	if err != nil {
		return false, err
	}
	return cnt >= lq.maxsize, nil
}

func (lq *SQueueLite) Prune() error {
	_, err := lq.conn.Exec("DELETE FROM Queue WHERE Status IN (?, ?)", DONE, FAILED)
	if err != nil {
		return err
	}
	return nil
}

func (lq *SQueueLite) Vacuum() error {
	_, err := lq.conn.Exec("VACUUM")
	return err
}

func (lq *SQueueLite) Transaction(mode string) (func(), error) {
	if mode != "DEFERRED" && mode != "IMMEDIATE" && mode != "EXCLUSIVE" {
		return nil, fmt.Errorf("Transaction mode '%s' is not valid", mode)
	}
	_, err := lq.conn.Exec(fmt.Sprintf("BEGIN %s", mode))
	if err != nil {
		return nil, err
	}
	return func() {
		if err != nil {
			lq.conn.Exec("ROLLBACK")
		} else {
			lq.conn.Exec("COMMIT")
		}
	}, nil
}

func (lq *SQueueLite) String() string {
	rows, err := lq.conn.Query("SELECT * FROM Queue LIMIT 3")
	if err != nil {
		return fmt.Sprintf("%T(Connection=%v)", lq, lq.conn)
	}
	defer rows.Close()
	var displayItems []Message
	for rows.Next() {
		var message Message
		if err := rows.Scan(&message.Data, &message.MessageID, &message.Status, &message.inTime, &message.lockTime, &message.doneTime); err != nil {
			return fmt.Sprintf("%T(Connection=%v)", lq, lq.conn)
		}
		displayItems = append(displayItems, message)
	}
	return fmt.Sprintf("%T(Connection=%v, items=%v)", lq, lq.conn, displayItems)
}

func (lq *SQueueLite) Close() error {
	return lq.conn.Close()
}
