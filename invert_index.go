package tns

import (
	"fmt"
	"log"
	"time"
)

type InvertIndex struct {
	TotalDocs int

	tokenMap     map[string]*Token
	reverseToken map[uint64]string

	// invert index map tokenID -> (docID, postingList)
	iiMap map[uint64]map[uint64]*PostingList

	store Store
}

func (ii *InvertIndex) WriteTo(store Store) {
	log.Println("start flush index")
	start := time.Now()
	i := 0
	for _, plMap := range ii.iiMap {
		for _, pl := range plMap {
			store.AddPostingList(pl)
			i++
		}
	}

	for _, tk := range ii.tokenMap {
		fmt.Printf("%s --> %v\n", tk.Value, tk.DocCount)
		store.UpdateToken(tk)
	}

	log.Printf("tokens: %d", len(ii.tokenMap))
	log.Printf("pl: %d", i)
	log.Printf("index flushed in %v\n", time.Now().Sub(start))
}

func (ii *InvertIndex) FetchPostingList(v string) (*Token, error) {
	return ii.store.GetToken(v)
}

func LoadInvertIndex(store Store) (*InvertIndex, error) {
	start := time.Now()
	ii := &InvertIndex{
		tokenMap: make(map[string]*Token),
		iiMap:    make(map[uint64]map[uint64]*PostingList),
	}

	var err error
	ii.TotalDocs, err = store.DocCount()
	if err != nil {
		return nil, err
	}

	// store.ScanToken(func(token *Token) {
	// 	//log.Printf("%s --> %v\n", token.Value, token.DocCount)
	// 	ii.tokenMap[token.Value] = &(*token)
	// })

	// i := 0
	// store.ScanPostingList(func(tokenID, docID uint64, pl []int) {
	// 	fmt.Printf("%v -> %v\n", docID, pl)
	// 	plMap, ok := ii.iiMap[tokenID]
	// 	if !ok {
	// 		plMap = make(map[uint64]*PostingList)
	// 		ii.iiMap[tokenID] = plMap
	// 	}

	// 	plMap[docID] = &PostingList{
	// 		PosList: pl,
	// 	}
	// 	i++
	// })

	// log.Printf("tokens: %d", len(ii.tokenMap))
	// log.Printf("pl: %d", i)
	log.Printf("index loaded in %v\n", time.Now().Sub(start))

	return ii, nil
}
