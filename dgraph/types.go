package dgraph

import (
	"fmt"
)

//SchemaField TODO
type SchemaField struct {
	Name string `json:"name,omitempty"`
}

func (m *SchemaField) String() string {
	return fmt.Sprintf("{Name: %v}", m.Name)
}

//SchemaType TODO
type SchemaType struct {
	Fields []*SchemaField `json:"fields,omitempty"`
	Name   string         `json:"name,omitempty"`
}

//SchemaTypes TODO
type SchemaTypes struct {
	Types []*SchemaType `json:"types,omitempty"`
}
