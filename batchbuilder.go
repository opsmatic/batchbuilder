// This is a simple helper to make building batched prepared queries less
// ridiculous. Initially build for CQL, I'm pretty sure it'll work for most
// SQL-like languages
//
// Any time a query constructor takes an interface{}, you can pass a func()
// (interface{}, error) instead. Batch.Join will execute this at batch
// construction time and use the return value, or return the error
package batchbuilder

import (
	"fmt"
	"strings"
)

type PreparedQuery struct {
	Query string
	Args  []interface{}
}

// Note this method applied to Inserts and Updates only
func (self PreparedQuery) WithTTL(seconds int) PreparedQuery {
	self.Query = fmt.Sprintf("%s USING TTL %d", self.Query, seconds)
	return self
}

func NewPreparedQuery(query string, args ...interface{}) PreparedQuery {
	return PreparedQuery{query, args}
}

// NewInsert creates an INSERT query
func NewInsert(table string, values map[string]interface{}) PreparedQuery {
	var cols, qs []string
	var vals []interface{}
	for col, value := range values {
		cols = append(cols, col)
		qs = append(qs, "?")
		vals = append(vals, value)
	}
	return NewPreparedQuery(fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", table, strings.Join(cols, ", "), strings.Join(qs, ", ")), vals...)
}

// NewUpdate creates an UPDATE query
func NewUpdate(table string, updates map[string]interface{}, wheres map[string]interface{}) PreparedQuery {
	var cols []string
	var upds []string
	var vals []interface{}
	for upd, value := range updates {
		upds = append(upds, fmt.Sprintf("%s = ?", upd))
		vals = append(vals, value)
	}
	for col, where := range wheres {
		cols = append(cols, fmt.Sprintf("%s = ?", col))
		vals = append(vals, where)
	}
	return NewPreparedQuery(fmt.Sprintf("UPDATE %s SET %s WHERE %s", table, strings.Join(upds, ", "), strings.Join(cols, " AND ")), vals...)
}

// NewDelete builds a delete query using ANDs. Disjunctive DELETEs are out of
// scope, for now anyway
func NewDelete(table string, wheres map[string]interface{}) PreparedQuery {
	var cols []string
	var vals []interface{}
	for col, where := range wheres {
		cols = append(cols, fmt.Sprintf("%s = ?", col))
		vals = append(vals, where)
	}
	return NewPreparedQuery(fmt.Sprintf("DELETE FROM %s WHERE %s", table, strings.Join(cols, " AND ")), vals...)
}

type Batch interface {
	AddQuery(query PreparedQuery) error
	Join(separator, startCmd, endCmd string) (string, []interface{}, error)
}

type ErrTooManyArgs struct {
	adding  int
	current int
	max     int
}

func (self ErrTooManyArgs) Error() string {
	return fmt.Sprintf("can't add query with %d args, batch already has %d (max %d)", self.adding, self.current, self.max)
}

const DEFAULT_MAX_ARGS = 65536

type BasicBatch struct {
	Queries   []PreparedQuery
	MaxArgs   int
	totalArgs int
}

func NewBasicBatch() *BasicBatch {
	return &BasicBatch{
		make([]PreparedQuery, 0),
		DEFAULT_MAX_ARGS,
		0,
	}
}

// AddQuery adds a query/args pair
func (self *BasicBatch) AddQuery(query PreparedQuery) (err error) {
	if self.MaxArgs > 0 && self.totalArgs+len(query.Args) > self.MaxArgs {
		err = ErrTooManyArgs{len(query.Args), self.totalArgs, self.MaxArgs}
		return
	}
	self.Queries = append(self.Queries, query)
	self.totalArgs += len(query.Args)
	return
}

// Join builds up a big string query and also returns all the arguments as one
// big slice in the correct order to match up with the ?s in the underlying
// queries.
// NOTE: if you're using a client/db combo that supports passing in lists of
// queries (e.g github.com/tux21b/gocql with cassandra >= 2.0), you can build
// up batch objects for that client by just accessing Batch.Queries directly.
func (self *BasicBatch) Join(separator, startCmd, endCmd string) (string, []interface{}, error) {
	var queries []string
	var allArgs []interface{}
	for _, preparedQuery := range self.Queries {
		queries = append(queries, preparedQuery.Query)
		for _, arg := range preparedQuery.Args {
			if f, ok := arg.(func() (interface{}, error)); ok {
				if argValue, err := f(); err != nil {
					return "", nil, fmt.Errorf("Unable to generate query argument at runtime: %s", err)
				} else {
					allArgs = append(allArgs, argValue)
				}
			} else {
				allArgs = append(allArgs, arg)
			}
		}
	}
	if startCmd != "" {
		queries = append([]string{startCmd}, queries...)
	}
	if endCmd != "" {
		queries = append(queries, endCmd)
	}
	return strings.Join(queries, separator), allArgs, nil
}
