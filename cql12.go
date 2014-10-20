package batchbuilder

import (
	"fmt"

	"github.com/gocql/gocql"
)

type Cql12Batch struct {
	Batch
	timestamp *int64 // pointer so it can be nil since 0 is a valid ts
}

func NewCql12Batch() *Cql12Batch {
	return &Cql12Batch{}
}

func (self *Cql12Batch) UsingTimestamp(ts int64) *Cql12Batch {
	self.timestamp = &ts
	return self
}

func (self *Cql12Batch) Apply(session *gocql.Session) (err error) {
	var (
		batchQuery string
		batchArgs  []interface{}
	)

	beginStatement := "BEGIN BATCH"
	if self.timestamp != nil {
		beginStatement = fmt.Sprintf("%s USING TIMESTAMP %d", beginStatement, *self.timestamp)
	}
	batchQuery, batchArgs, err = self.Join("\n", beginStatement, "APPLY BATCH")
	if err != nil {
		return
	}

	err = session.Query(batchQuery, batchArgs...).Exec()
	return
}
