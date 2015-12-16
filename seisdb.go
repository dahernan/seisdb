// SeisDB is a Hexastore
// http://www.vldb.org/pvldb/1/1453965.pdf
package seisdb

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/boltdb/bolt"
)

type SeisDB struct {
	// store
	bo *bolt.DB

	bucket string
}

// sextuples
// spo:dahernan:is-friend-of:agonzalezro
// sop:dahernan:agonzalezro:is-friend-of
// ops:agonzalezro:is-friend-of:dahernan
// osp:agonzalezro:dahernan:is-friend-of
// pso:is-friend-of:dahernan:agonzalezro
// pos:is-friend-of:agonzalezro:dahernan

type Triple struct {
	// subject
	S string
	// predicate
	P string
	// object
	O string
}

func NewTriple(tupla string) Triple {
	// parse this
	// spo:dahernan:is-friend-of:agonzalezro
	split := strings.SplitN(tupla, ":", 4)
	s := 1
	o := 2
	p := 3
	if len(split[0]) != 3 {
		return Triple{}
	}
	for index, ch := range split[0] {
		switch ch {
		case 's':
			s = index + 1
		case 'o':
			o = index + 1
		case 'p':
			p = index + 1
		}
	}
	return Triple{S: split[s], O: split[o], P: split[p]}

}

func (t Triple) Sextuple() []string {
	return []string{
		fmt.Sprintf("spo:%v:%v:%v", t.S, t.P, t.O),
		fmt.Sprintf("sop:%v:%v:%v", t.S, t.O, t.P),
		fmt.Sprintf("ops:%v:%v:%v", t.O, t.P, t.S),
		fmt.Sprintf("osp:%v:%v:%v", t.O, t.S, t.P),
		fmt.Sprintf("pso:%v:%v:%v", t.P, t.S, t.O),
		fmt.Sprintf("pos:%v:%v:%v", t.P, t.O, t.S),
	}
}

func Open(path string) (*SeisDB, error) {
	bucket := "default"
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return err
		}
		return nil
	})
	return &SeisDB{bo: db, bucket: bucket}, err
}

func (sdb *SeisDB) Close() {
	sdb.bo.Close()
}

func (sdb *SeisDB) Update(t Triple) error {
	return sdb.bo.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(sdb.bucket))
		st := t.Sextuple()
		for _, v := range st {
			err := b.Put([]byte(v), []byte{})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (sdb *SeisDB) Find(pre string) ([]Triple, error) {
	r := []Triple{}
	err := sdb.bo.View(func(tx *bolt.Tx) error {
		c := tx.Bucket([]byte(sdb.bucket)).Cursor()
		prefix := []byte(pre)
		for k, _ := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, _ = c.Next() {
			r = append(r, NewTriple(string(k)))
		}
		return nil
	})

	return r, err
}
