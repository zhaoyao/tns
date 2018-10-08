package tns

import (
	"github.com/huichen/sego"
	"github.com/yanyiwu/gojieba"
)

type Term struct {
	Text  string
	Start int
}

type Tokenizer interface {
	Tokenzie(text string, searchMode bool) []Term
}

func NewSegoTokenizer(dictPath string) Tokenizer {
	seg := &sego.Segmenter{}
	seg.LoadDictionary(dictPath)
	return &segoTokenizer{
		seg: seg,
	}
}

func NewJiebaTokenizer(dictPath ...string) Tokenizer {
	j := gojieba.NewJieba(dictPath...)
	return &jiebaTokenizer{
		j: j,
	}
}

type segoTokenizer struct {
	seg *sego.Segmenter
}

func (t *segoTokenizer) Tokenzie(text string, searchMode bool) []Term {
	s := t.seg.InternalSegment([]byte(text), searchMode)
	terms := make([]Term, len(s))
	for i, seg := range s {
		terms[i].Text = seg.Token().Text()
		terms[i].Start = seg.Start()
	}
	return terms
}

type jiebaTokenizer struct {
	j *gojieba.Jieba
}

func (t *jiebaTokenizer) Tokenzie(text string, searchMode bool) []Term {
	m := gojieba.DefaultMode
	if searchMode {
		m = gojieba.SearchMode
	}

	words := t.j.Tokenize(text, m, false)

	keywords := t.j.Extract(text, 100)
	kwm := make(map[string]bool)
	for _, w := range keywords {
		kwm[w] = true
	}

	var terms []Term
	for _, w := range words {
		if _, ok := kwm[w.Str]; !ok {
			continue
		}

		terms = append(terms, Term{w.Str, w.Start})
	}

	return terms
}
