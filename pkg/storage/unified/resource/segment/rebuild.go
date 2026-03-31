package segment

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/blevesearch/bleve/v2/analysis"
	"github.com/blevesearch/bleve/v2/document"
	"github.com/blevesearch/bleve/v2/registry"
	index "github.com/blevesearch/bleve_index_api"

	// Register the keyword analyzer.
	_ "github.com/blevesearch/bleve/v2/analysis/analyzer/keyword"
)

var (
	kwOnce sync.Once
	kwVal  analysis.Analyzer
)

func getKeywordAnalyzer() analysis.Analyzer {
	kwOnce.Do(func() {
		cache := registry.NewCache()
		a, err := registry.AnalyzerBuild("keyword", nil, cache)
		if err != nil {
			panic(fmt.Sprintf("failed to build keyword analyzer: %v", err))
		}
		kwVal = a.(analysis.Analyzer)
	})
	return kwVal
}

// StoredDoc holds data extracted from a segment's stored fields for one document.
type StoredDoc struct {
	DocID  string // _id field
	RV     int64  // parsed from _rv
	Action string // "created", "updated", "deleted"
	Folder string
	Source []byte
	IsTomb bool // _tombstone marker present
}

// ReadAllDocs reads every document from a segment reader, extracting stored fields.
func ReadAllDocs(reader *SegmentReader) ([]StoredDoc, error) {
	n := reader.NumDocs()
	docs := make([]StoredDoc, 0, n)

	for i := uint64(0); i < uint64(n); i++ {
		docID, err := reader.DocID(i)
		if err != nil {
			continue
		}

		var d StoredDoc
		d.DocID = docID

		err = reader.VisitStoredFields(i, func(field string, _ byte, value []byte, _ []uint64) bool {
			switch field {
			case "_rv":
				d.RV, _ = strconv.ParseInt(string(value), 10, 64)
			case "action":
				d.Action = string(value)
			case "folder":
				d.Folder = string(value)
			case "_source":
				d.Source = make([]byte, len(value))
				copy(d.Source, value)
			case "_tombstone":
				d.IsTomb = true
			}
			return true
		})
		if err != nil {
			continue
		}

		docs = append(docs, d)
	}

	return docs, nil
}

// RebuildWithLatest reads all docs from source segments, drops tombstones,
// and rebuilds a single segment with the `_latest` field set correctly:
// only the highest-RV non-deleted version of each _id gets _latest="1".
//
// Returns nil segment if all docs are tombstones.
func RebuildWithLatest(ctx context.Context, builder *SegmentBuilder, readers []*SegmentReader, highestRV uint64) (*Segment, error) {
	// Collect all docs from all segments.
	var allDocs []StoredDoc
	for _, reader := range readers {
		docs, err := ReadAllDocs(reader)
		if err != nil {
			return nil, fmt.Errorf("read docs: %w", err)
		}
		allDocs = append(allDocs, docs...)
	}

	// Drop tombstones.
	live := make([]StoredDoc, 0, len(allDocs))
	for _, d := range allDocs {
		if !d.IsTomb {
			live = append(live, d)
		}
	}
	if len(live) == 0 {
		return nil, nil
	}

	// Find the latest non-deleted RV per _id.
	latestRVByID := make(map[string]int64)
	for _, d := range live {
		if d.Action != "deleted" && d.RV > latestRVByID[d.DocID] {
			latestRVByID[d.DocID] = d.RV
		}
	}

	// Sort by (_id, rv) so the output segment's _id dictionary is ordered and
	// _latest postings appear in _id-sorted order — enabling streaming List.
	sort.Slice(live, func(i, j int) bool {
		if live[i].DocID != live[j].DocID {
			return live[i].DocID < live[j].DocID
		}
		return live[i].RV < live[j].RV
	})

	// Build bleve documents with _latest set correctly.
	bleveDocs := make([]*document.Document, 0, len(live))
	for _, d := range live {
		isLatest := d.Action != "deleted" && d.RV == latestRVByID[d.DocID]
		bleveDocs = append(bleveDocs, rebuildBleveDoc(d, isLatest))
	}

	return builder.BuildSegment(ctx, bleveDocs, highestRV)
}

// rebuildBleveDoc reconstructs a bleve document from stored fields.
func rebuildBleveDoc(d StoredDoc, isLatest bool) *document.Document {
	kw := getKeywordAnalyzer()
	doc := document.NewDocument(d.DocID)

	// Parse namespace/name from _id: {group}/{resource}/{namespace}/{name} or {group}/{resource}/{name}
	parts := strings.Split(d.DocID, "/")
	var namespace, name string
	switch len(parts) {
	case 4:
		namespace, name = parts[2], parts[3]
	case 3:
		name = parts[2]
	}

	if namespace != "" {
		doc.AddField(document.NewTextFieldCustom("namespace", nil, []byte(namespace), index.IndexField, kw))
	}
	doc.AddField(document.NewTextFieldCustom("name", nil, []byte(name), index.IndexField, kw))
	doc.AddField(document.NewNumericFieldWithIndexingOptions("resource_version", nil, float64(d.RV), index.IndexField))
	doc.AddField(document.NewTextFieldWithIndexingOptions("_rv", nil, []byte(strconv.FormatInt(d.RV, 10)), index.StoreField))
	doc.AddField(document.NewTextFieldCustom("action", nil, []byte(d.Action), index.IndexField|index.StoreField, kw))
	if d.Folder != "" {
		doc.AddField(document.NewTextFieldCustom("folder", nil, []byte(d.Folder), index.IndexField|index.StoreField, kw))
	}
	if d.Source != nil {
		doc.AddField(document.NewTextFieldWithIndexingOptions("_source", nil, d.Source, index.StoreField))
	}
	if isLatest {
		doc.AddField(document.NewTextFieldCustom("_latest", nil, []byte("1"), index.IndexField, kw))
	}

	return doc
}
