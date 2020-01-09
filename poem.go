package poem

import (
	"bytes"
	"encoding/gob"
	"os"
	"strconv"
	"time"
)

type Query interface {
	RunWrite(session *Session) (*Result, error)
}

type Session struct {
}

type ConnectOpts struct {
	Address string
}

type Result struct {
}

type DB struct {
	Db string
}

type Table struct {
	Db string
	Table string
}

type DBCreateQuery struct {
	Db string
}

type TableCreateQuery struct {
	Db string
	Table string
}

type InsertQuery struct {
	Db string
	Table string
	Value interface{}
}

func Connect(options ConnectOpts) (*Session, error) {
	return &Session{}, nil
}

func DBCreate(db string) Query {
	return DBCreateQuery{Db: db}
}

func (db DB) TableCreate(table string) Query {
	query := TableCreateQuery{
		Db:    db.Db,
		Table: table,
	}

	return query
}

func (db DB) Table(tableName string) Table {
	table := Table{
		Db:    db.Db,
		Table: tableName,
	}

	return table
}

func (db DB) Dump(destination string) error {
	return nil
}

func (table Table) Insert(value interface{}) Query {
	query := InsertQuery{
		Db:    table.Db,
		Table: table.Table,
		Value: value,
	}

	return query
}

func (query DBCreateQuery) RunWrite(session *Session) (*Result, error) {
	createError := os.MkdirAll("poem/"+query.Db, os.ModePerm)
	if createError != nil {
		return nil, createError
	}

	return &Result{}, nil
}

func (query TableCreateQuery) RunWrite(session *Session) (*Result, error) {
	createError := os.MkdirAll("poem/"+query.Db+"/"+query.Table, os.ModePerm)
	if createError != nil {
		return nil, createError
	}

	return &Result{}, nil
}

func (query InsertQuery) RunWrite(session *Session) (*Result, error) {
	mkdirError := os.MkdirAll("poem/"+query.Db+"/"+query.Table, os.ModePerm)
	if mkdirError != nil {
		return nil, mkdirError
	}

	id := makeID()

	file, createError := os.Create("poem/"+query.Db+"/"+query.Table+"/"+id)
	if createError != nil {
		return nil, createError
	}

	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)

	marshalError := encoder.Encode(query.Value)
	if marshalError != nil {
		return nil, marshalError
	}

	file.Write(buffer.Bytes())
	file.Close()

	return &Result{}, nil
}

func makeID() string {
	currentTime := time.Now()
	timeNumber := currentTime.UnixNano()
	connectionID := strconv.Itoa(int(timeNumber))
	return connectionID
}