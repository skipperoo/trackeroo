package service

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"
	"trackeroo-backend/internal/logger"

	_ "modernc.org/sqlite"
)

type PQ struct {
	dbCon  *sql.DB
	dbLock sync.Mutex
}

var queues map[string]*PQ = map[string]*PQ{}

func InitQueue(name string) {
	queueDir := "status/queues/" + name
	queueFile := queueDir + "/" + name + ".db"
	err := os.MkdirAll(queueDir, 0777)
	if err != nil {
		logger.Error("Cannot open data queue, %s", err)
	}

	if _, err := os.Stat(queueFile); os.IsNotExist(err) {
		dbFile, _ := os.Create(queueFile)
		dbFile.Close()
	}
	queue := new(PQ)
	queue.dbLock.Lock()
	defer queue.dbLock.Unlock()
	queue.dbCon, err = sql.Open("sqlite", queueFile)
	if err != nil {
		logger.Error("Cannot open database, %s", err)
	}
	queue.dbCon.Exec("BEGIN TRANSACTION")
	queue.dbCon.Exec("CREATE TABLE IF NOT EXISTS " + name + "_queue (id INTEGER PRIMARY KEY AUTOINCREMENT, data BLOB, status INTEGER)")
	queue.dbCon.Exec("COMMIT")
	queues[name] = queue
}

func EnqueueData(name, devID, tag, payload string) {
	p := queues[name]
	if p == nil {
		logger.Error("Queue %s does not exists", name)
		return
	}
	data := devID + "/" + tag + "/" + payload

	insertQuery := "INSERT INTO " + name + "_queue(data, status) VALUES(?,?);"
	p.dbLock.Lock()

	defer p.dbLock.Unlock()

	tx, err := p.dbCon.Begin()
	if err != nil {
		logger.Error("%s", err)
	}
	stm, err := tx.Prepare(insertQuery)
	if err != nil {
		logger.Error("%s", err)
	}
	_, err = stm.Exec(data, 0)
	// _, err = p.dbCon.Exec(insertQuery, data, 0)
	if err != nil {
		tx.Rollback()
		logger.Error("%s", err)
		return
	}
	stm.Close()
	err = tx.Commit()
	if err != nil {
		logger.Error("logger.Error committing, %s", err)
	}
}

func DequeueData(name string) (int, any, error) {
	dequeueQuery := "SELECT * FROM " + name + "_queue WHERE status = 0 ORDER BY id;"
	updateStatusQuery := "UPDATE " + name + "_queue SET status = 1 WHERE id = ?;"
	p := queues[name]
	if p == nil {
		logger.Error("Queue %s does not exists", name)
		return -1, nil, fmt.Errorf("queue does not exists")
	}
	var id int
	var data any
	var status int
	p.dbLock.Lock()
	defer p.dbLock.Unlock()

	tx, err := p.dbCon.Begin()
	if err != nil {
		logger.Error("logger.Error beginning the transaction, %s", err)
		return -1, nil, fmt.Errorf("cannot start the transaction")
	}
	stm, err := tx.Prepare(dequeueQuery)
	if err != nil {
		logger.Error("logger.Error dequeuing, %s", err)
	}
	// res := tx.QueryRow(dequeueQuery)
	res := stm.QueryRow()
	err = res.Scan(&id, &data, &status)
	if err != nil {
		logger.Error("logger.Error reading the row, %s", err)
		id = -1
	}
	stm.Close()
	if id == -1 {
		err = tx.Rollback()
		if err != nil {
			logger.Error("logger.Error rolling back, %s", err)
		}
		return id, nil, nil
	}
	stm, err = tx.Prepare(updateStatusQuery)
	if err != nil {
		logger.Error("logger.Error error preparing the update, %s", err)
		tx.Rollback()
		return -1, nil, fmt.Errorf("cannot dequeue")
	}
	_, err = stm.Exec(id)
	stm.Close()
	if err != nil {
		logger.Error("logger.Error updating the status, %s", err)
		tx.Rollback()
		return -1, nil, fmt.Errorf("cannot update status after dequeue")
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return -1, nil, fmt.Errorf("error committing, %s", err)
	}
	return id, data, nil
}

func setStatus(name string, id int, status int) error {
	updateStatusQuery := "UPDATE " + name + "_queue SET status = ? WHERE id = ?;"
	p := queues[name]
	if p == nil {
		logger.Error("Queue %s does not exists", name)
		return fmt.Errorf("queue does not exists")
	}
	p.dbLock.Lock()
	defer p.dbLock.Unlock()
	tx, err := p.dbCon.Begin()
	if err != nil {
		logger.Error("logger.Error starting the transaction, %s", err)
	}
	stm, err := tx.Prepare(updateStatusQuery)
	if err != nil {
		logger.Error("logger.Error preparing the update, %s", err)
	}
	_, err = stm.Exec(status, id)
	if err != nil {
		logger.Error("logger.Error updating the status, %s", err)
		tx.Rollback()
		stm.Close()
		return fmt.Errorf("cannot set the status")
	}

	stm.Close()
	err = tx.Commit()
	if err != nil {
		logger.Error("logger.Error committing, %s", err)
	}
	return nil
}

func Size(name string) (int, error) {
	sizeQuery := "SELECT COUNT(*) FROM " + name + "_queue WHERE status = 0 ORDER BY id;"
	p := queues[name]
	if p == nil {
		logger.Error("Queue %s does not exists", name)
		return -1, fmt.Errorf("queue does not exists")
	}
	p.dbLock.Lock()
	defer p.dbLock.Unlock()
	tx, err := p.dbCon.Begin()
	if err != nil {
		logger.Error("logger.Error starting the transaction, %s", err)
	}
	stm, _ := tx.Prepare(sizeQuery)
	var id int
	row := stm.QueryRow()
	err = row.Scan(&id)
	tx.Commit()
	if err != nil {
		return -1, fmt.Errorf("error reading the size, %s", err)
	}
	return id, nil
}

func Ack(name string, id int) error {
	return setStatus(name, id, 5)
}

func Unack(name string, id int) error {
	return setStatus(name, id, 0)
}

func QueuesCleanUp() {
	logger.Debug("Started queues cleanup")
	for {
		for queueName := range queues {
			// if condition, close db, reopen
			logger.Debug("Cleaning %s", queueName)
			queue := queues[queueName]
			queue.dbLock.Lock()
			tx, err := queue.dbCon.Begin()
			if err != nil {
				logger.Error("logger.Error starting the transaction, retrying later: %s", err)
				queue.dbLock.Unlock()
				continue
			}
			_, err = tx.Exec("DELETE FROM " + queueName + "_queue WHERE status = 5")
			if err != nil {
				logger.Error("Deleting acked rows, %s", err)
			}

			tx.Commit()
			_, err = queue.dbCon.Exec("VACUUM")
			if err != nil {
				logger.Error("logger.Error executing VACUUM: %s", err)
			}
			queue.dbCon.Close()
			queue.dbCon, _ = sql.Open("sqlite", "status/queues/"+queueName+"/"+queueName+".db")
			queue.dbLock.Unlock()
			logger.Debug("Cleaned %s", queueName)
		}
		time.Sleep(time.Second * 30)
	}
}
