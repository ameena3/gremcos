package api

import (
	"fmt"

	"github.com/ameena3/gremcos/interfaces"
)

type simpleQueryBuilder struct {
	value string
}

func NewSimpleQB(format string, a ...interface{}) interfaces.QueryBuilder {
	return &simpleQueryBuilder{
		value: fmt.Sprintf(format, a...),
	}
}

func (sqb *simpleQueryBuilder) String() string {
	return sqb.value
}
