package tns

import (
	"encoding/xml"
	"os"

	"github.com/rcrowley/go-metrics"
)

type WikiPage struct {
	Title string `xml:"title"`
	Text  string `xml:"revision>text"`
}

type WikiPageEntry struct {
	Page *WikiPage
	Err  error
}

var (
	DocParsed = metrics.NewRegisteredMeter("doc_par", metrics.DefaultRegistry)
)

func LoadWikiXML(path string, n int) (chan WikiPageEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	dec := xml.NewDecoder(f)
	println("decoder created")

	ch := make(chan WikiPageEntry)

	go func() {
		defer f.Close()

		for n > 0 {
			// Read tokens from the XML document in a stream.
			t, _ := dec.Token()
			if t == nil {
				break
			}
			// Inspect the type of the token just read.
			switch se := t.(type) {
			case xml.StartElement:
				// If we just read a StartElement token
				// ...and its name is "page"
				if se.Name.Local == "page" {
					p := &WikiPage{}
					// decode a whole chunk of following XML into the
					// variable p which is a Page (se above)

					err := dec.DecodeElement(p, &se)
					DocParsed.Mark(1)

					if err != nil {
						ch <- WikiPageEntry{Err: err}
					} else {
						ch <- WikiPageEntry{Page: p}
					}

					n--
				}
			}
		}
	}()

	return ch, nil
}
