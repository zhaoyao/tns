package main

import (
	"log"
	"os"

	"github.com/huichen/sego"
	"github.com/zhaoyao/tns"
	bolt "go.etcd.io/bbolt"
)

var (
	store tns.Store
	seg   sego.Segmenter
	t     tns.Tokenizer = tns.NewJiebaTokenizer()
)

func main() {

	//t = tns.NewSegoTokenizer("data/dictionary.txt")

	//	seg.LoadDictionary("data/dictionary.txt")

	// go metrics.LogScaled(metrics.DefaultRegistry, 5*time.Second, time.Millisecond,
	// 	log.New(os.Stderr, "metrics: ", log.Lmicroseconds))

	buildIndex(2000000)
	// ii, store := loadIndex()
	// searcher := tns.NewSearcher(ii, t, store)

	// r := bufio.NewReader(os.Stdin)
	// for {
	// 	fmt.Printf("> ")
	// 	b, _, _ := r.ReadLine()

	// 	p := strings.Split(string(b), ",")

	// 	var q, sf string
	// 	explain := false
	// 	switch len(p) {
	// 	case 3:
	// 		q = p[0]
	// 		sf = p[1]
	// 		explain = true
	// 	case 2:
	// 		q = p[0]
	// 		sf = p[1]
	// 	default:
	// 		q = p[0]
	// 	}

	// 	hits := searcher.Search(q, sf, 20)

	// 	fmt.Printf("%d docs matched in %v\n", hits.Total, hits.Duration)
	// 	for i, h := range hits.Hits {
	// 		fmt.Printf("%d -> %s %v\n", i+1, h.Doc.Fields["Title"], h.Score)
	// 		if explain {
	// 			fmt.Println(h.Explain)
	// 		}
	// 	}

	// 	fmt.Println("==================================")
	// }

}

func loadIndex() (*tns.InvertIndex, tns.Store) {
	db, err := bolt.Open("./wiki_jieba.db", 0666, nil)
	if err != nil {
		log.Fatal(err)
	}

	store, err = tns.NewBoltStore(db)
	if err != nil {
		log.Fatal(err)
	}
	//defer store.Close()

	ii, err := tns.LoadInvertIndex(store)
	if err != nil {
		log.Fatal(err)
	}

	return ii, store
}

func buildIndex(total int) {
	xmlPath := os.Args[1]

	//total := 1024

	ch, err := tns.LoadWikiXML(xmlPath, total)
	if err != nil {
		log.Fatal(err)
	}

	store, err = tns.CreateBoltStore("./wiki_jieba.db")
	if err != nil {
		log.Fatal(err)
	}
	defer store.Close()

	indexer := tns.NewIndexer(t, store)

	processed := 0
	for p := range ch {
		if p.Err != nil {
			log.Fatal(err)
		}
		//log.Printf("indexing %+#v\n", p.Page)

		doc := &tns.Document{
			Index: "wiki",
			Fields: map[string]string{
				"Title": p.Page.Title,
				"Text":  p.Page.Text,
			}}

		if err := indexer.AddDoc(doc); err != nil {
			log.Fatal(err)
		}

		processed++
		if processed == total {
			break
		}
	}

	ii := indexer.Build()
	ii.WriteTo(store)
}
