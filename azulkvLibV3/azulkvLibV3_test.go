package azulkv3

import (
//	"log"
//	"fmt"
	"testing"
//	"os"
//    "math/rand"
//    "time"
)

func TestDb(t* testing.T) {

	db, err := InitDb("testDb", "tstdb.db", false)
	if err != nil {t.Errorf("error -- InitDb: %v", err)}

	err = db.CloseDb()
	if err != nil {t.Errorf("error -- CloseDb: %v", err)}

}

/*
//func TestAddEntry(t *testing.T) {

	_, err := os.Stat("testDb")
	if err == nil {
		err1 := os.RemoveAll("testDb")
		if err1 != nil {t.Errorf("error -- could not remove files: %v", err1)}
	}
	db, err := InitDB("testDb", false)
	if err != nil {t.Errorf("error -- InitKV: %v", err)}

//	dbtab := 
	err = db.AddEntry("key1", "val1")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}


	if db.Keys[0] != "key1" {t.Errorf("keys do not agree: %s is not %s!", (*kv.Keys)[0], "key1")}

}

func TestGetEntry(t *testing.T) {
	kv, err := InitKV("testDb", false)
	if err != nil {t.Errorf("error -- InitKV: %v", err)}

	err = kv.AddEntry("key1", "val1")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}

	if (*kv.Keys)[0] != "key1" {t.Errorf("keys do not agree: %s is not %s!", (*kv.Keys)[0], "key1")}
	if (*kv.Entries) != 1 {t.Errorf("invalid Entries: %d!", (*kv.Entries))}

	idx, valstr := kv.GetVal("key1")
	if valstr != "val1" {t.Errorf("values do not agree: %s is not %s!", valstr, "val1")}
	if idx<0 || idx>(*kv.Entries) {t.Errorf("invalid index: %d!",idx)}

	valstr,err = kv.GetValByIdx(0)
	if err != nil {t.Errorf("error -- GetValByIdx: %v", err)}
	if valstr != "val1" {t.Errorf("values do not agree: %s is not %s!", valstr, "val1")}

	hash := GetHash([]byte("key1"))
	idx, valstr = kv.GetValByHash(hash)
	if valstr != "val1" {t.Errorf("values do not agree: %s is not %s!", valstr, "val1")}
	if idx<0 || idx>(*kv.Entries) {t.Errorf("invalid index: %d!",idx)}


}


func TestUpdEntry(t *testing.T) {
	kv, err := InitKV("testDb", false)
	if err != nil {t.Errorf("error -- InitKV: %v", err)}

	err = kv.AddEntry("key1", "val1")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}

	if (*kv.Keys)[0] != "key1" {t.Errorf("keys do not agree: %s is not %s!", (*kv.Keys)[0], "key1")}

	idx := kv.FindKey("key1")
	if idx == -1 {t.Errorf("error -- FindKey: %d key1 not found!", idx)}

	err = kv.UpdEntryByIdx(idx, "val1New")
	if err != nil {t.Errorf("error -- UpdEntry: %v", err)}

	valstr,err := kv.GetValByIdx(idx)
	if err != nil {t.Errorf("error -- GetValByIdx: %v", err)}
	if valstr != "val1New" {t.Errorf("values do not agree: %s is not %s!", valstr, "val1New")}
	
}

func TestDelEntry(t *testing.T) {

	kv, err := InitKV("testDb", false)
	if err != nil {t.Errorf("error -- InitKV: %v", err)}

	err = kv.AddEntry("key1", "val1")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}

	if (*kv.Keys)[0] != "key1" {t.Errorf("keys do not agree: %s is not %s!", (*kv.Keys)[0], "key1")}

	idx := kv.FindKey("key1")
	if idx == -1 {t.Errorf("error -- FindKey: %d key1 not found!", idx)}

	err = kv.DelEntry(idx)
	if err != nil {t.Errorf("error -- DelEntry: %v", err)}

	idx = kv.FindKey("key1")
	if idx != -1 {t.Errorf("error -- FindKey: %d key1 not deleted!", idx)}

}

func TestBckupAndLoad(t *testing.T) {

	kv, err := InitKV("testDb", false)
	if err != nil {t.Errorf("error -- InitKV: %v", err)}

	err = kv.FillRan(5)
	if err != nil {t.Errorf("error -- FillRan: %v", err)}

	err = kv.Backup("testBackup.dat")
	if err != nil {t.Errorf("error -- Backup: %v", err)}

	kvnew, err := InitKV("testDb", false)
	if err != nil {t.Errorf("error -- Load: %v", err)}

	err = kvnew.Load("azulkvBase.dat")
	if err != nil {t.Errorf("error -- Load: %v", err)}

	if (*kv.Entries) != (*kvnew.Entries) {t.Errorf("error entries do not match kv: %d kvnew: %d", (*kv.Entries), (*kvnew.Entries))}
	for i:=0; i< (*kv.Entries); i++ {
		if (*kv.Keys)[i] != (*kvnew.Keys)[i] {
			t.Errorf("error -- no key match at idx[%d] key: %s keynew: %s",i, (*kv.Keys)[i], (*kvnew.Keys)[i])
		}
	}
//	err = os.Remove("testDb/testBackup.dat")
//	if err != nil {t.Errorf("error -- Remove: %v", err)}

}

func TestGet(t *testing.T) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

//	os.RemoveAll("testDb")
	numEntries := 100
	kv, err := InitKV("testDb", false)
    if err != nil {t.Errorf("error -- InitKV: %v", err)}

    err = kv.FillRan(numEntries)
    if err != nil {t.Errorf("error -- FillRan: %v", err)}

    err = kv.Backup("testBackup.dat")
    if err != nil {t.Errorf("error -- Backup: %v", err)}

		kidx := seededRand.Intn(numEntries)
		keyStr := (*kv.Keys)[kidx]
		idx, valstr := kv.GetVal(keyStr)
		if idx != kidx  {t.Errorf("values do not agree: %d is not %d!", kidx, idx)}
		if len(valstr) < 1 {t.Errorf("invalid valstr!")}

}


func BenchmarkGet(b *testing.B) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

	os.RemoveAll("testDbNew")

	numEntries := 100
	kv, err := InitKV("testDbNew", false)
    if err != nil {log.Fatalf("error -- InitKV: %v", err)}

    err = kv.FillRan(numEntries)
    if err != nil {log.Fatalf("error -- FillRan: %v", err)}

//    err = kv.Backup("testDbNew_Backup.dat")
//    if err != nil {log.Fatalf("error -- Backup: %v", err)}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		kidx := seededRand.Intn(numEntries)
		keyStr := (*kv.Keys)[kidx]
		idx, valstr := kv.GetVal(keyStr)
		if idx != kidx  {log.Fatalf("values do not agree[%d]: %d is not %d!", n, kidx, idx)}
		if len(valstr) < 1 {log.Fatalf("invalid valstr!")}
	}
}
*/
