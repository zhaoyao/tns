package tns

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"

	bolt "go.etcd.io/bbolt"
)

type Store interface {
	AddDoc(doc *Document) error
	GetDoc(id uint64) (*Document, error)
	DelDoc(id uint64) error
	DocCount() (int, error)

	AllocToken(token string) (tk *Token, err error)
	GetToken(token string) (*Token, error)
	UpdateToken(token *Token) error

	AddPostingList(pl *PostingList) error

	ScanToken(f func(token *Token)) error

	ScanPostingListByToken(tokenID uint64, f func(pl *PostingList)) error
	ScanPostingList(f func(pl *PostingList)) error

	Close() error
}

type BoltStore struct {
	db *bolt.DB

	docPending   []*Document
	tokenPending []*Token
	plPending    []*PostingList
}

var (
	docBucket   = []byte("doc")
	tokenBucket = []byte("token")
	iiBucket    = []byte("ii")

	flushTreshold = 4096
)

func CreateBoltStore(path string) (Store, error) {
	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return nil, err
	}
	db.NoSync = true

	return NewBoltStore(db)
}

func NewBoltStore(db *bolt.DB) (Store, error) {
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(docBucket)
		_, err = tx.CreateBucketIfNotExists(tokenBucket)
		_, err = tx.CreateBucketIfNotExists(iiBucket)
		return err
	})

	return &BoltStore{db: db}, nil
}

func (s *BoltStore) DocCount() (int, error) {
	var c int
	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(docBucket)
		c = b.Stats().KeyN
		return nil
	})
	return c, err
}

func (s *BoltStore) AddDoc(doc *Document) error {
	s.db.Update(func(tx *bolt.Tx) error {
		doc.ID, _ = tx.Bucket(docBucket).NextSequence()
		return nil
	})

	if len(s.docPending) < flushTreshold {
		s.docPending = append(s.docPending, doc)
		return nil
	}

	return s.db.Update(s.flushDoc)
}

func (s *BoltStore) flushDoc(t *bolt.Tx) error {
	b := t.Bucket(docBucket)

	for _, doc := range s.docPending {
		if doc.ID == 0 {
			return errors.New("doc no id")
		}

		body, err := json.Marshal(doc.Fields)
		if err != nil {
			return err
		}

		if err := b.Put(itob(doc.ID), body); err != nil {
			return err
		}

	}

	s.docPending = nil
	return nil
}

var ErrDocNotFound = errors.New("doc not found")

func (s *BoltStore) GetDoc(id uint64) (*Document, error) {
	doc := &Document{ID: id, Fields: make(map[string]string)}

	err := s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(docBucket)
		valBytes := b.Get(itob(id))
		if valBytes == nil {
			return ErrDocNotFound
		}

		err := json.Unmarshal(valBytes, &doc.Fields)
		if err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return doc, nil
}

func (s *BoltStore) DelDoc(id uint64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(docBucket)
		return b.Delete(itob(id))
	})
}

func (s *BoltStore) AllocToken(token string) (tk *Token, err error) {
	tk = &Token{}

	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(tokenBucket)

		tk.ID, err = b.NextSequence()
		return err
	})

	tk.Value = token
	return tk, err
}

func (s *BoltStore) GetToken(token string) (tk *Token, err error) {
	tk = &Token{}

	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(tokenBucket)

		tkVal := b.Get([]byte(token))
		if tkVal == nil {
			tk.ID, err = b.NextSequence()
			if err != nil {
				return err
			}

			j, _ := json.Marshal(tk)
			return b.Put([]byte(token), j)
		}

		return json.Unmarshal(tkVal, tk)
	})

	tk.Value = token
	return tk, err
}

func (s *BoltStore) UpdateToken(tk *Token) error {
	if len(s.tokenPending) < flushTreshold {
		s.tokenPending = append(s.tokenPending, tk)
		return nil
	}

	return s.db.Update(s.flushToken)
}

func (s *BoltStore) flushToken(t *bolt.Tx) error {
	b := t.Bucket(tokenBucket)

	for _, tk := range s.tokenPending {
		key := []byte(tk.Value)
		tk.Value = ""
		bytes, err := json.Marshal(tk)
		if err != nil {
			_ = t.Rollback()
			return err
		}

		if err := b.Put(key, bytes); err != nil {
			_ = t.Rollback()
			return err
		}

	}

	s.tokenPending = nil
	return nil
}

func (s *BoltStore) AddPostingList(pl *PostingList) error {
	s.plPending = append(s.plPending, pl)

	if len(s.plPending) < flushTreshold {
		return nil
	}

	return s.db.Update(s.flushPostingList)
}

func (s *BoltStore) flushPostingList(t *bolt.Tx) error {
	b := t.Bucket(iiBucket)

	for _, pl := range s.plPending {
		key := append(itob(pl.TokenID), itob(pl.DocID)...)

		val, _ := json.Marshal(pl)

		// buf := bytes.NewBuffer(make([]byte, 4*len(pl.PosList)))
		// for _, pos := range pl.PosList {
		// 	binary.Write(buf, binary.BigEndian, uint32(pos))
		// }

		if err := b.Put(key, val); err != nil {
			return err
		}
	}

	s.plPending = nil
	return nil
}

func (s *BoltStore) ScanToken(f func(token *Token)) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(tokenBucket)

		return b.ForEach(func(k, v []byte) error {
			//var tokenID uint64
			//binary.Read(bytes.NewBuffer(k), binary.BigEndian, &tokenID)

			var tk Token
			json.Unmarshal(v, &tk)
			tk.Value = string(k)
			f(&tk)

			return nil
		})
	})
}

func (s *BoltStore) ScanPostingListByToken(tokenID uint64, f func(pl *PostingList)) error {
	return s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(iiBucket).Cursor()
		prefix := itob(tokenID)
		for k, v := c.Seek(prefix); k != nil; k, v = c.Next() {
			if bytes.HasPrefix(k, prefix) {
				if err := applyPostList(k, v, f); err != nil {
					return err
				}
			}
		}

		return nil
	})
}

func (s *BoltStore) ScanPostingList(f func(pl *PostingList)) error {
	return s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(iiBucket)

		return b.ForEach(func(k, v []byte) error {
			applyPostList(k, v, f)
			return nil
		})
	})
}

func applyPostList(k, v []byte, f func(pl *PostingList)) error {
	kb := bytes.NewBuffer(k)
	// vb := bytes.NewBuffer(v)

	var tokenID, docID uint64
	if err := binary.Read(kb, binary.BigEndian, &tokenID); err != nil {
		return err
	}
	if err := binary.Read(kb, binary.BigEndian, &docID); err != nil {
		return err
	}

	pl := &PostingList{}
	json.Unmarshal(v, pl)

	//fmt.Printf("len(v)=%d v/4=%d\n", len(v), len(v)/4)
	// pl := make([]int, len(v)/4)
	// i := 0
	// for {
	// 	var pos uint32
	// 	binary.Read(vb, binary.BigEndian, &pos)
	// 	//	fmt.Printf("i=%d v=%d\n", i, pos)
	// 	pl[i] = int(pos)
	// 	i++
	// 	if i == len(v)/4 {
	// 		break
	// 	}
	// }

	f(pl)
	return nil
}

func (s *BoltStore) Close() error {
	s.db.Update(func(tx *bolt.Tx) error {
		if err := s.flushDoc(tx); err != nil {
			return err
		}

		if err := s.flushToken(tx); err != nil {
			return err
		}

		if err := s.flushPostingList(tx); err != nil {
			return err
		}

		return nil
	})

	s.db.Close()
	return nil
}

func itob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}
