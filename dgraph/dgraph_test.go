package dgraph

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"testing"
	"time"
)

type Content struct {
	UID             string   `json:"uid,omitempty"`
	Label           string   `json:"label,omitempty"`
	Value           string   `json:"value,omitempty"`
	ContentSequence int      `json:"content_sequence"`
	DType           []string `json:"dgraph.type,omitempty"`
}

func (m *Content) String() string {
	return fmt.Sprintf("Content{Label: %v, Value: %v, ContentSequence: %v, DType: %v}", m.Label, m.Value, m.ContentSequence, m.DType)
}

type Document struct {
	UID         string     `json:"uid,omitempty"`
	Hash        string     `json:"hash,omitempty"`
	CreatedDate time.Time  `json:"created_date,omitempty"`
	Creator     string     `json:"creator,omitempty"`
	Contents    []*Content `json:"contents,omitempty"`
	DType       []string   `json:"dgraph.type,omitempty"`
}

func (m *Document) String() string {
	return fmt.Sprintf("Content{Hash: %v, CreatedDate: %v, Creator: %v, Contents: %v, DType: %v}", m.Hash, m.CreatedDate, m.Creator, m.Contents, m.DType)
}

var dgraph *Dgraph

// TestMain will exec each test, one by one
func TestMain(m *testing.M) {
	beforeAll()
	// exec test and this returns an exit code to pass to os
	retCode := m.Run()
	afterAll()
	// If exit code is distinct of zero,
	// the test will be failed (red)
	os.Exit(retCode)
}

func beforeAll() {
	var err error
	dgraph, err = New("")
	if err != nil {
		log.Fatalf("Unable to create dgraph: %v", err)
	}
	err = dgraph.DropAll()
	if err != nil {
		log.Fatalf("Unable to drop all: %v", err)
	}
}

func afterAll() {
	dgraph.Close()
}

func TestUpdateSchema(t *testing.T) {
	err := dgraph.UpdateSchema(`
		type Document {
				hash
				created_date
				creator
				contents
		}
		
		type Content {
			label
			value
			content_sequence
		}
		
		hash: string @index(exact) .
		created_date: datetime .
		creator: string @index(term) .
		contents: [uid] .

		label: string @index(term) .
		value: string @index(term) .
		type: string @index(term) .
		content_sequence: int .
		
	`)
	if err != nil {
		t.Fatalf("UpdateSchema failed: %v", err)
	}

	missing, err := dgraph.MissingTypes([]string{"Document", "Content"})
	if err != nil {
		t.Fatalf("MissingTypes failed: %v", err)
	}
	if len(missing) > 0 {
		t.Fatalf("Expected [] MissingTypes found: %v", missing)
	}
	fieldMap, err := dgraph.GetTypeFieldMap("Document")
	if err != nil {
		t.Fatalf("GetTypeFieldMap failed: %v", err)
	}
	expectedFieldMap := map[string]*SchemaField{
		"hash":         {"hash"},
		"created_date": {"created_date"},
		"creator":      {"creator"},
		"contents":     {"contents"},
	}
	if !reflect.DeepEqual(fieldMap, expectedFieldMap) {
		t.Fatalf("Expected fieldMap: %v found: %v", expectedFieldMap, fieldMap)
	}

}

func TestMutations(t *testing.T) {
	createdDate, err := time.Parse("2006-01-02T15:04:05.000", "2020-08-24T16:59:58.500")
	if err != nil {
		t.Fatalf("Date creation failed: %v", err)
	}
	doc := &Document{
		Hash:        "363bccee26dd6ffaa8f107d8ceb6a666a34f1de978c57dcad487475d107b79e5",
		Creator:     "johnnyhypha1",
		CreatedDate: createdDate,
		DType:       []string{"Document"},
		Contents: []*Content{
			{
				Label:           "content_group_name1",
				Value:           "My content group #1",
				ContentSequence: 0,
				DType:           []string{"Content"},
			},
			{
				Label:           "content_group_name2",
				Value:           "My content group #2",
				ContentSequence: 1,
				DType:           []string{"Content"},
			},
		},
	}

	resp, err := dgraph.MutateJSON(doc, false)
	if err != nil {
		t.Fatalf("MutateJSON failed: %v", err)
	}
	t.Logf("Mutate Response: %v", resp)

	var docs struct {
		Documents []*Document
	}
	err = dgraph.Query(
		`query documents ($hash: string){
			documents(func: eq(hash, $hash)){
				hash 
				creator
				created_date
				dgraph.type
				contents {
					label
					value
					content_sequence
					dgraph.type
				}
			}
		}`,
		map[string]string{"$hash": "363bccee26dd6ffaa8f107d8ceb6a666a34f1de978c57dcad487475d107b79e5"},
		&docs,
	)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	if len(docs.Documents) != 1 {
		t.Fatalf("Expected Documents len to be 1 found: %v", len(docs.Documents))
	}
	if !reflect.DeepEqual(docs.Documents[0], doc) {
		t.Fatalf("Expected Document %v found: %v", doc, docs.Documents[0])
	}

	err = dgraph.Query(
		`query documents ($hash: string){
			documents(func: eq(hash, $hash)){
				uid
				contents {
					uid
				}
			}
		}`,
		map[string]string{"$hash": "363bccee26dd6ffaa8f107d8ceb6a666a34f1de978c57dcad487475d107b79e5"},
		&docs,
	)

	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(docs.Documents) != 1 {
		t.Fatalf("Expected Documents len to be 1 found: %v", len(docs.Documents))
	}

	if len(docs.Documents[0].Contents) != 2 {
		t.Fatalf("Expected Contents len to be 2 found: %v", len(docs.Documents[0].Contents))
	}

	content := &Content{
		Label:           "content_group_name3",
		Value:           "My content group #3",
		ContentSequence: 2,
		DType:           []string{"Content"},
	}

	resp, err = dgraph.MutateJSON(content, false)
	if err != nil {
		t.Fatalf("MutateJSON failed: %v", resp)
	}
	t.Logf("Mutate Response: %v", resp)
	for _, uid := range resp.GetUids() {
		t.Logf("Adding edge contents to %v, with: %v", docs.Documents[0].UID, uid)
		_, err := dgraph.MutateEdge(docs.Documents[0].UID, uid, "contents", false)
		if err != nil {
			t.Fatalf("MutateEdge(Add) failed: %v", err)
		}
	}

	err = dgraph.Query(
		`query documents ($hash: string){
			documents(func: eq(hash, $hash)){
				uid
				contents {
					uid
				}
			}
		}`,
		map[string]string{"$hash": "363bccee26dd6ffaa8f107d8ceb6a666a34f1de978c57dcad487475d107b79e5"},
		&docs,
	)

	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(docs.Documents[0].Contents) != 3 {
		t.Fatalf("Expected Contents len to be 3 found: %v", len(docs.Documents[0].Contents))
	}

	t.Logf("Deleting edge contents from %v, with: %v", docs.Documents[0].UID, docs.Documents[0].Contents[0].UID)
	_, err = dgraph.MutateEdge(docs.Documents[0].UID, docs.Documents[0].Contents[0].UID, "contents", true)
	if err != nil {
		t.Fatalf("MutateEdge(Add) failed: %v", err)
	}

	err = dgraph.Query(
		`query documents ($hash: string){
			documents(func: eq(hash, $hash)){
				uid
				contents {
					uid
				}
			}
		}`,
		map[string]string{"$hash": "363bccee26dd6ffaa8f107d8ceb6a666a34f1de978c57dcad487475d107b79e5"},
		&docs,
	)

	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(docs.Documents[0].Contents) != 2 {
		t.Fatalf("Expected Contents len to be 2 found: %v", len(docs.Documents[0].Contents))
	}

	_, err = dgraph.DeleteNode(docs.Documents[0].UID)
	if err != nil {
		t.Fatalf("Delete node failed: %v", err)
	}

	err = dgraph.Query(
		`query documents ($hash: string){
			documents(func: eq(hash, $hash)){
				uid
			}
		}`,
		map[string]string{"$hash": "363bccee26dd6ffaa8f107d8ceb6a666a34f1de978c57dcad487475d107b79e5"},
		&docs,
	)

	if len(docs.Documents) != 0 {
		t.Fatalf("Expected Documents len to be 0 found: %v", len(docs.Documents))
	}

}
