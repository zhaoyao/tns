package tns

type IndexSpec struct {
	Name   string
	Fields []*FieldSpec
}

type FieldSpec struct {
	Name string
}

type Document struct {
	ID     uint64
	Index  string
	Fields map[string]string
}

type InvertedIndex struct {
	h map[string]Token
}

type Token struct {
	ID       uint64
	Value    string
	DocCount int
	PosCount int
}

// PostList 记录某个词元 (token) 在某个文档(DocID) 中的位置信息
type PostingList struct {
	TokenID uint64
	DocID   uint64
	DocLen  int
	PosList []int // Freq = len(PosList)
}
