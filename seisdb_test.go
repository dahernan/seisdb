package seisdb

import "testing"

func TestXX(t *testing.T) {

	sdb, err := Open("my.db")
	if err != nil {
		t.Fatal("wrong", err)
	}
	defer sdb.Close()

	sdb.Update(Triple{S: "dahernan", P: "is-friend-of", O: "agonzalezro"})
	sdb.Update(Triple{S: "dahernan", P: "is-friend-of", O: "ibo"})

	sdb.Update(Triple{S: "dahernan", P: "likes", O: "kung-fu"})
	sdb.Update(Triple{S: "dahernan", P: "likes", O: "cycling"})

	sdb.Update(Triple{S: "ibo", P: "likes", O: "music"})

	t.Log("All dahernan relations")
	rr, err := sdb.Find("spo:dahernan")
	if err != nil {
		t.Fatal("wrong", err)
	}
	for _, v := range rr {
		t.Log(v)
	}

	t.Log("All friends")
	rr, err = sdb.Find("pos:is-friend-of")
	if err != nil {
		t.Fatal("wrong", err)
	}
	for _, v := range rr {
		t.Log(v)
	}

	t.Log(sdb)

}
