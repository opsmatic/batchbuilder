// This is a simple helper to make building batched prepared queries less
// ridiculous. Initially build for CQL, I'm pretty sure it'll work for most
// SQL-like languages
package batchbuilder

import (
	"fmt"
	"strings"
)

type PreparedQuery struct {
	Query string
	Args  []interface{}
}

func NewPreparedQuery(query string, vars ...interface{}) PreparedQuery {
	return PreparedQuery{query, vars}
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

// NewDelete builds a delete query using ANDs. Disjunctive DELETEs are out of
// scope, for now anyway
func NewDelete(table string, values map[string]interface{}) PreparedQuery {
	var cols []string
	var vals []interface{}
	for col, value := range values {
		cols = append(cols, fmt.Sprintf("%s = ?", col))
		vals = append(vals, value)
	}
	return NewPreparedQuery(fmt.Sprintf("DELETE FROM %s WHERE %s", table, strings.Join(cols, " AND ")), vals...)
}

type Batch struct {
	Queries []PreparedQuery
}

// AddQuery adds a query/args pair
func (self *Batch) AddQuery(query PreparedQuery) {
	self.Queries = append(self.Queries, query)
}

// Join builds up a big string query and also returns all the arguments as one
// big slice in the correct order to match up with the ?s in the underlying
// queries.
// NOTE: if you're using a client/db combo that supports passing in lists of
// queries (e.g github.com/tux21b/gocql with cassandra >= 2.0), you can build
// up batch objects for that client by just accessing Batch.Queries directly.
func (self *Batch) Join(separator, startCmd, endCmd string) (string, []interface{}) {
	var queries []string
	var allArgs []interface{}
	for _, preparedQuery := range self.Queries {
		queries = append(queries, preparedQuery.Query)
		allArgs = append(allArgs, preparedQuery.Args...)
	}
	if startCmd != "" {
		queries = append([]string{startCmd}, queries...)
	}
	if endCmd != "" {
		queries = append(queries, endCmd)
	}
	return strings.Join(queries, separator), allArgs
}