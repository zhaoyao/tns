package tns_test

import (
	"testing"

	"github.com/zhaoyao/tns"
)

func TestLoadWiki(t *testing.T) {
	ch, err := tns.LoadWikiXML("data/zhwiki-latest-pages-articles.xml")
	if err != nil {
		t.Fatal(err)
	}

	c := <-ch
	if c.Err != nil {
		t.Fatal(err)
	}

	t.Logf("parsed page: %+v=\n", c.Page)
}
