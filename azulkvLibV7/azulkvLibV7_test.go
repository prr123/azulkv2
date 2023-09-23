package azulkv7

import (
	"log"
//	"fmt"
	"testing"
	"os"
    "math/rand"
    "time"
)

func TestDb(t* testing.T) {

	dbobj, err := InitDb("testDb", "tstdb.db", false)
	if err != nil {t.Errorf("error -- InitDb: %v", err)}

	err = dbobj.CloseDb()
	if err != nil {t.Errorf("error -- CloseDb: %v", err)}

}


func TestAddEntry(t *testing.T) {

	_, err := os.Stat("testDb/testDb.db")
	if err == nil {
		err1 := os.Remove("testDb/testDb.db")
		if err1 != nil {t.Errorf("error -- could not remove files: %v", err1)}
	}
	db, err := InitDb("testDb", "testDb.db", false)
	if err != nil {t.Errorf("error -- InitKV: %v", err)}

//	log.Println("*** after initdb ****")
//	db.PrintDb(0,db.Entries)

	err = db.AddEntry("key1", "val1")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}

//	log.Println("*** after AddEntry ****")
//	db.PrintDb(0,db.Entries)

//	log.Printf("Entries: %d\n", *db.Entries)
//	log.Printf("Entries: %d Keys[%d] %s\n", *db.Entries, len(*db.Keys), (*db.Keys)[*db.Entries -1])

	if db.Keys[0] != "key1" {t.Errorf("keys do not agree: %s is not %s!", db.Keys[0], "key1")}


	idx, valstr := db.GetVal("key1")
	if valstr != "val1" {t.Errorf("values do not agree: %s is not %s!", valstr, "val1")}
	if idx != 0 {t.Errorf("idx is not 0: %d!",idx)}


//	if idx<0 || idx>(*kv.Entries) {t.Errorf("invalid index: %d!",idx)}

}

/*
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
*/

func TestUpdEntry(t *testing.T) {
	db, err := InitDb("testDb", "testDb.db", false)
	if err != nil {t.Errorf("error -- InitKV: %v", err)}

	db.Clean()
	err = db.AddEntry("key1", "val1")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}

	if db.Keys[0] != "key1" {t.Errorf("keys do not agree: %s is not %s!", db.Keys[0], "key1")}

	err = db.AddEntry("key2", "val2")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}

	idx := db.FindKey("key2")
	if idx == -1 {t.Errorf("error -- FindKey: %d key2 not found!", idx)}
	if idx != 1 {t.Errorf("error -- FindKey: idx %d for key2 should be 1!", idx)}


	err = db.UpdEntryByIdx(idx, "val1New")
	if err != nil {t.Errorf("error -- UpdEntry: %v", err)}

	valstr,err := db.GetValByIdx(idx)
	if err != nil {t.Errorf("error -- GetValByIdx: %v", err)}
	if valstr != "val1New" {t.Errorf("values do not agree: %s is not %s!", valstr, "val1New")}

	db.CloseDb()
}


func TestDelEntry(t *testing.T) {

	db, err := InitDb("testDb", "testDb.db", false)
	if err != nil {t.Errorf("error -- InitKV: %v", err)}

	db.Clean()

	err = db.AddEntry("key3", "val3")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}

	if db.Entries != 1 {t.Errorf("db.Entries should be 1: is %d!", db.Entries)}
	if db.Keys[0] != "key3" {t.Errorf("keys do not agree: %s is not %s!", db.Keys[0], "key3")}

	idx := db.FindKey("key3")
	if idx == -1 {t.Errorf("error -- FindKey: %d key3 not found!", idx)}

	err = db.DelEntryByIdx(idx)
	if err != nil {t.Errorf("error -- DelEntry: %v", err)}

	idx = db.FindKey("key3")
	if idx != -1 {t.Errorf("error -- FindKey: %d key3 not deleted!", idx)}

	db.CloseDb()
}

func TestGet(t *testing.T) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

	numEntries := 10

	db, err := InitDb("testDb", "test2Db.db", false)
	if err != nil {t.Errorf("error -- InitDb: %v", err)}

	db.Clean()

    err = db.FillRan(numEntries)
    if err != nil {t.Errorf("error -- FillRan: %v", err)}

//    err = db.Backup("testBackup.dat")
//    if err != nil {t.Errorf("error -- Backup: %v", err)}

	for i:= 0; i< 20; i++ {
		kidx := seededRand.Intn(numEntries)
		keyStr := db.Keys[kidx]
		idx, valstr := db.GetVal(keyStr)
		if idx != kidx  {t.Errorf("values do not agree: %d is not %d!", kidx, idx)}
		if len(valstr) < 1 {t.Errorf("invalid valstr!")}
	}

	db.CloseDb()
}


func TestBckupAndLoad(t *testing.T) {

	db, err := InitDb("testDb", "testBckupDb.db", false)
	if err != nil {t.Errorf("error -- InitKV: %v", err)}

	db.Clean()

	err = db.FillRan(5)
	if err != nil {t.Errorf("error -- FillRan: %v", err)}

	err = db.Backup("test2BckupDb.db")
	if err != nil {t.Errorf("error -- Backup: %v", err)}

//	err = db.Close()

	dbnew, err := InitDb("testDb", "test2BckupDb.db", false)
	if err != nil {t.Errorf("error -- InitKV: %v", err)}

	if  db.Entries!= dbnew.Entries {t.Errorf("error entries do not match kv: %d kvnew: %d", db.Entries, dbnew.Entries)}
	for i:=0; i< db.Entries; i++ {
		if db.Keys[i] != dbnew.Keys[i] {
			t.Errorf("error -- no key match at idx[%d] key: %s keynew: %s",i, db.Keys[i], dbnew.Keys[i])
		}
	}
//	err = os.Remove("testDb/testBackup.dat")
//	if err != nil {t.Errorf("error -- Remove: %v", err)}

	db.CloseDb()
	dbnew.CloseDb()
}



func BenchmarkGet100(b *testing.B) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

	os.RemoveAll("testDbBench")

	numEntries := 100
	db, err := InitDb("testDbBench", "testDb100.db", false)
	if err != nil {log.Fatalf("error -- InitDb: %v", err)}

	db.Clean()

    err = db.FillRan(numEntries)
    if err != nil {log.Fatalf("error -- FillRan: %v", err)}

//    err = kv.Backup("testDbNew_Backup.dat")
//    if err != nil {log.Fatalf("error -- Backup: %v", err)}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		kidx := seededRand.Intn(numEntries)
		keyStr := db.Keys[kidx]
		idx, valstr := db.GetVal(keyStr)
		if idx != kidx  {log.Fatalf("values do not agree[%d]: %d is not %d!", n, kidx, idx)}
		if len(valstr) < 1 {log.Fatalf("invalid valstr!")}
	}
}


func BenchmarkGet200(b *testing.B) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

	numEntries := 200
	db, err := InitDb("testDbBench", "testDb200.db", false)
	if err != nil {log.Fatalf("error -- InitDb: %v", err)}

	db.Clean()

    err = db.FillRan(numEntries)
    if err != nil {log.Fatalf("error -- FillRan: %v", err)}

//    err = kv.Backup("testDbNew_Backup.dat")
//    if err != nil {log.Fatalf("error -- Backup: %v", err)}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		kidx := seededRand.Intn(numEntries)
		keyStr := db.Keys[kidx]
		idx, valstr := db.GetVal(keyStr)
		if idx != kidx  {log.Fatalf("values do not agree[%d]: %d is not %d!", n, kidx, idx)}
		if len(valstr) < 1 {log.Fatalf("invalid valstr!")}
	}
}

func BenchmarkGet500(b *testing.B) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

	numEntries := 500
	db, err := InitDb("testDbBench", "testDb500.db", false)
	if err != nil {log.Fatalf("error -- InitDb: %v", err)}

	db.Clean()

    err = db.FillRan(numEntries)
    if err != nil {log.Fatalf("error -- FillRan: %v", err)}

//    err = kv.Backup("testDbNew_Backup.dat")
//    if err != nil {log.Fatalf("error -- Backup: %v", err)}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		kidx := seededRand.Intn(numEntries)
		keyStr := db.Keys[kidx]
		idx, valstr := db.GetVal(keyStr)
		if idx != kidx  {log.Fatalf("values do not agree[%d]: %d is not %d!", n, kidx, idx)}
		if len(valstr) < 1 {log.Fatalf("invalid valstr!")}
	}
}

func BenchmarkGet1000(b *testing.B) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

	numEntries := 1000
	db, err := InitDb("testDbBench", "testDb1000.db", false)
	if err != nil {log.Fatalf("error -- InitDb: %v", err)}

	db.Clean()

    err = db.FillRan(numEntries)
    if err != nil {log.Fatalf("error -- FillRan: %v", err)}

//    err = kv.Backup("testDbNew_Backup.dat")
//    if err != nil {log.Fatalf("error -- Backup: %v", err)}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		kidx := seededRand.Intn(numEntries)
		keyStr := db.Keys[kidx]
		idx, valstr := db.GetVal(keyStr)
		if idx != kidx  {log.Fatalf("values do not agree[%d]: %d is not %d!", n, kidx, idx)}
		if len(valstr) < 1 {log.Fatalf("invalid valstr!")}
	}
}

