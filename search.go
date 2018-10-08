package tns

import (
	"fmt"
	"log"
	"math"
	"sort"
	"time"
)

type Searcher struct {
	ii    *InvertIndex
	t     Tokenizer
	store Store
}

type termHit struct {
	t  *Token
	pl []int
}

func NewSearcher(ii *InvertIndex, t Tokenizer, store Store) *Searcher {
	return &Searcher{
		ii:    ii,
		t:     t,
		store: store,
	}
}

type Hit struct {
	docID     uint64
	docLen    int
	hitTokens []*termHit

	Doc  *Document
	Term string
	//PosList []int
	Score   float64
	Explain string
}

type TopHits struct {
	Total    int
	Duration time.Duration
	Hits     []*Hit
}

type ScoreFunc func(*Hit, []*Token, int) (float64, string)

func tf_idf(h *Hit, tokens []*Token, totalDocs int) (float64, string) {
	var score float64
	explain := ""

	for _, t := range h.hitTokens {

		tf := float64(len(t.pl)) / float64(h.docLen)

		idf := math.Log2(float64(totalDocs) / float64(t.t.DocCount+1))

		score += float64(tf) * idf

		explain += fmt.Sprintf("[%s](tf=%v * idf(total/doc=%v)=%v): %v, ", t.t.Value, tf, t.t.DocCount+1, idf, score)
	}

	//fmt.Printf("tf: %v\n", tf)
	//	fmt.Printf("log2(%v/%v) %v\n", float64(totalDocs), float64(term.DocCount+1), idf)
	return score, explain
}

func lucene_tf_idf(h *Hit, tokens []*Token, totalDocs int) (float64, string) {
	var score float64
	explain := ""

	for _, t := range h.hitTokens {

		idf := math.Log2(float64(totalDocs) / float64(t.t.DocCount+1))

		tf := math.Sqrt(float64(len(t.pl)))

		fieldNorms := 1 / math.Sqrt(float64(h.docLen))

		score += tf * idf * fieldNorms

		explain += fmt.Sprintf("[%s](tf=%v * idf(total/doc=%v)=%v norms=%v): %v, ",
			t.t.Value, tf, t.t.DocCount+1, idf, fieldNorms, score)
	}

	//fmt.Printf("tf: %v\n", tf)
	//	fmt.Printf("log2(%v/%v) %v\n", float64(totalDocs), float64(term.DocCount+1), idf)
	return score, explain
}

func bm25(h *Hit, tokens []*Token, totalDocs int) (float64, string) {
	var score float64
	explain := ""

	for _, t := range h.hitTokens {

		idf := math.Log2(float64(totalDocs) / float64(t.t.DocCount+1))

		// ((k + 1) * tf) / (k + tf)
		tf := float64(len(t.pl))
		k := 1.2
		tfScore := (float64(k+1) * tf) / (k + tf)

		fieldNorms := 1 / math.Sqrt(float64(len(h.Doc.Fields["Text"])))

		score += tfScore * idf * fieldNorms

		explain += fmt.Sprintf("[%s](tf=%v * idf(total/doc=%v)=%v norms=%v): %v, ",
			t.t.Value, tfScore, t.t.DocCount+1, idf, fieldNorms, score)
	}

	//fmt.Printf("tf: %v\n", tf)
	//	fmt.Printf("log2(%v/%v) %v\n", float64(totalDocs), float64(term.DocCount+1), idf)
	return score, explain
}

func (s *Searcher) Search(q string, sf string, n int) *TopHits {
	start := time.Now()
	terms := s.t.Tokenzie(q, true)

	var hits []*Hit

	// scoreFunc := tf_idf
	// scoreFunc := lucene_tf_idf
	var scoreFunc ScoreFunc
	switch sf {
	case "bm25":
		scoreFunc = bm25
	case "lucene":
		scoreFunc = lucene_tf_idf
	default:
		scoreFunc = tf_idf
	}

	docs := make(map[uint64]*Hit)

	for _, term := range terms {
		//	t, ok := s.ii.tokenMap[term.Text]
		t, err := s.store.GetToken(term.Text)
		fmt.Printf("token: %v %v %v\n", term.Text, t.ID, err)

		if err == nil {
			matched := 0
			err = s.store.ScanPostingListByToken(t.ID, func(pl *PostingList) {
				//fmt.Printf("\t%v %v --> %v\n", tokenID, docID, posList)
				matched++
				h, ok := docs[pl.DocID]
				if ok {
					h.hitTokens = append(h.hitTokens, &termHit{t: t, pl: pl.PosList})
				} else {
					h = &Hit{
						docID:     pl.DocID,
						hitTokens: []*termHit{&termHit{t: t, pl: pl.PosList}},
						docLen:    pl.DocLen,
						//Term:      term.Text,
						// PosList:   posList.PosList,
					}
					docs[pl.DocID] = h
				}
			})

			fmt.Printf("matched: %v\n", matched)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	var err error
	for _, h := range docs {
		h.Doc, err = s.store.GetDoc(h.docID)
		if err == nil {
			h.Score, h.Explain = scoreFunc(h, nil, s.ii.TotalDocs)
			hits = append(hits, h)
		}
	}

	sort.Slice(hits, func(i, j int) bool { return hits[i].Score >= hits[j].Score })

	result := &TopHits{
		Total:    len(hits),
		Hits:     hits,
		Duration: time.Now().Sub(start),
	}

	if len(hits) > n {
		result.Hits = hits[:n]
	}

	return result
}
