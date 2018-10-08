package tns

import (
	"fmt"
	"log"
	"time"

	"github.com/rcrowley/go-metrics"
)

var (
	//	DocAdded      = metrics.NewRegisteredMeter("docs_added", nil)
	AddSegTimer   = metrics.NewRegisteredTimer("segments_added", nil)
	IndexSegments = metrics.NewRegisteredHistogram("content_tokens", nil, metrics.NewUniformSample(512))
	AddDocTimer   = metrics.NewRegisteredTimer("docs_added", nil)
	//SegmentTimer  = metrics.NewRegisteredTimer("make_segments", nil)

	TokenPostingListKeptInMemory = 40960
)

type Indexer struct {
	//seg   *sego.Segmenter
	//	jieba *gojieba.Jieba
	t     Tokenizer
	store Store

	tokenMap map[string]*Token

	// invert index map tokenID -> (docID, postingList)
	iiMap map[uint64]map[uint64]*PostingList

	count          int64
	totalDocLength int64
}

func NewIndexer(t Tokenizer, store Store) *Indexer {
	return &Indexer{
		t:        t,
		store:    store,
		tokenMap: make(map[string]*Token),
		iiMap:    make(map[uint64]map[uint64]*PostingList),
	}
}

func (i *Indexer) AddDoc(doc *Document) (err error) {
	start := time.Now()
	err = i.store.AddDoc(doc)
	if err != nil {
		return err
	}
	AddDocTimer.UpdateSince(start)

	for _, val := range doc.Fields {
		i.totalDocLength += int64(len(val))
		if err := i.addTextToPosting(doc.ID, val); err != nil {
			return err
		}
	}

	if len(i.iiMap) >= TokenPostingListKeptInMemory {
		i.flushPostingList()
	}

	//AddDocTimer.UpdateSince(start)
	i.count++
	fmt.Printf("\r%d doc indexed, avg length: %v", i.count, float64(i.totalDocLength)/float64(i.count))
	return nil
}

func (i *Indexer) flushPostingList() (err error) {
	start := time.Now()

	count := 0
	for _, plMap := range i.iiMap {
		for _, pl := range plMap {
			i.store.AddPostingList(pl)
			count++
		}
	}

	log.Printf("%d posting list flushed in %v\n", count, time.Now().Sub(start))
	i.iiMap = make(map[uint64]map[uint64]*PostingList)
	return nil
}

func (i *Indexer) Build() *InvertIndex {
	return &InvertIndex{
		tokenMap: i.tokenMap,
		iiMap:    i.iiMap,
	}
}

func (i *Indexer) addTextToPosting(docID uint64, text string) error {
	//	start := time.Now()
	//segs := i.seg.Segment([]byte(text))
	terms := i.t.Tokenzie(text, false)

	//SegmentTimer.UpdateSince(start)
	IndexSegments.Update(int64(len(terms)))
	//fmt.Printf("len(txt)=%d tokens=%d\n", len(text), len(segs))
	for _, term := range terms {
		start := time.Now()
		if err := i.addTermToPosting(docID, text, &term); err != nil {
			return err
		}
		AddSegTimer.UpdateSince(start)
	}

	return nil
}

func (i *Indexer) addTermToPosting(docID uint64, text string, term *Term) error {
	t, err := i.lookupToken(term.Text)
	if err != nil {
		return err
	}

	// append to ii
	plMap, ok := i.iiMap[t.ID]
	if !ok {
		plMap = make(map[uint64]*PostingList)
		i.iiMap[t.ID] = plMap
	}

	pl, ok := plMap[docID]
	if !ok {
		pl = &PostingList{
			TokenID: t.ID,
			DocID:   docID,
			DocLen:  len(text),
		}
		plMap[docID] = pl
		t.DocCount++
	}
	t.PosCount++

	//sego.SegmentsToString
	pl.PosList = append(pl.PosList, term.Start)
	return nil
}

func (i *Indexer) lookupToken(text string) (*Token, error) {
	v, ok := i.tokenMap[text]
	if ok {
		return v, nil
	}

	var err error
	v, err = i.store.AllocToken(text)
	if err != nil {
		return nil, err
	}

	i.tokenMap[text] = v
	return v, nil
}
