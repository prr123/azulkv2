package azulkvMap

import (
	"log"
//	"fmt"
	"testing"
	"os"
    "math/rand"
    "time"
)

func TestDb(t* testing.T) {

	dbobj, err := InitDb("testMDb", "tstdb.db", false)
	if err != nil {t.Errorf("error -- InitDb: %v", err)}

//	log.Println("Closing")
	err = dbobj.CloseDb()
	if err != nil {t.Errorf("error -- CloseDb: %v", err)}
//	log.Println("End Closing")

}


func TestAddEntry(t *testing.T) {

	_, err := os.Stat("testMDb/testMDb.db")
	if err == nil {
		err1 := os.Remove("testMDb/testMDb.db")
		if err1 != nil {t.Errorf("error -- could not remove files: %v", err1)}
	}
	db, err := InitDb("testMDb", "testMDb.db", false)
	if err != nil {t.Errorf("error -- InitKV: %v", err)}

//	log.Println("*** initdb completed ****")
//	db.PrintDb(0,db.Entries)
	db.Clean()
//	log.Println("*** db cleaned ****")

	err = db.AddEntry("key1", "val1")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}
//	log.Println("addentry completed")

//	log.Println("*** after AddEntry ****")
//	db.PrintDb(0,db.Entries)

//	log.Printf("Entries: %d\n", *db.Entries)
//	log.Printf("Entries: %d Keys[%d] %s\n", *db.Entries, len(*db.Keys), (*db.Keys)[*db.Entries -1])

	if err1 :=db.FindKey("key1"); err1 != nil {t.Errorf("key not found!")}

	valstr, err := db.GetVal("key1")
	if err != nil {t.Errorf("GetVal err: %v!",err)}
	if valstr != "val1" {t.Errorf("values do not agree: %s is not %s!", valstr, "val1")}

}


func TestUpdEntry(t *testing.T) {
	db, err := InitDb("testMDb", "testMDb.db", false)
	if err != nil {t.Errorf("error -- InitdB: %v", err)}

	err = db.AddEntry("key1", "val1")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}

	if err1 :=db.FindKey("key1"); err1 != nil {t.Errorf("key \"key1\" not found!")}


	err = db.AddEntry("key2", "val2")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}

	if err1 :=db.FindKey("key2"); err1 != nil {t.Errorf("key \"key2\" not found!")}

	err = db.UpdEntry("key1", "val1New")
	if err != nil {t.Errorf("error -- UpdEntry: %v", err)}

	valstr, err := db.GetVal("key1")
	if err != nil {t.Errorf("GetVal err: %v!",err)}
	if valstr != "val1New" {t.Errorf("values do not agree: %s is not %s!", valstr, "val1New")}

	db.CloseDb()
}


func TestDelEntry(t *testing.T) {

	db, err := InitDb("testMDb", "testMDb.db", false)
	if err != nil {t.Errorf("error -- InitKV: %v", err)}

	db.Clean()
	err = db.AddEntry("key3", "val3")
	if err != nil {t.Errorf("error -- AddEntry: %v", err)}

	if len(db.KV) != 1 {t.Errorf("db.Entries should be 1: is %d!", len(db.KV))}

	if err1 :=db.FindKey("key3"); err1 != nil {t.Errorf("key \"key3\" not found!")}

	err = db.DelEntry("key3")
	if err != nil {t.Errorf("error -- DelEntry: %v", err)}

	if err1 :=db.FindKey("key3"); err1 == nil {t.Errorf("key \"key3\" found sfter del!")}

	db.CloseDb()
}

func TestGet(t *testing.T) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

	numEntries := 10

	db, err := InitDb("testMDb", "testM2Db.db", false)
	if err != nil {t.Errorf("error -- InitDb: %v", err)}

	db.Clean()

    err = db.FillRan(numEntries)
    if err != nil {t.Errorf("error -- FillRan: %v", err)}

	keyList := make([]string, len(db.KV))
	valList := make([]string, len(db.KV))
	cnt:=0
	for k, v := range(db.KV) {
		keyList[cnt] = k
		valList[cnt] = v
		cnt++
	}

	for i:= 0; i< 30; i++ {
		kidx := seededRand.Intn(numEntries)
		keyStr := keyList[kidx]
		valstr, err := db.GetVal(keyStr)
		if err != nil {t.Errorf("GetVal %s: %v", keyStr, err)}
		if valstr != valList[kidx]	{t.Errorf("values do not agree: %s is not %s!",valstr, valList[kidx])}
	}

	db.CloseDb()
}



func TestBckupAndLoad(t *testing.T) {

	db, err := InitDb("testMDb", "testMBckupDb.db", false)
	if err != nil {t.Errorf("error -- Initdb: %v", err)}

	db.Clean()

	err = db.FillRan(5)
	if err != nil {t.Errorf("error -- FillRan: %v", err)}


	err = db.Backup("testM2BckupDb.db")
	if err != nil {t.Errorf("error -- Backup: %v", err)}

//	err = db.Close()

	dbnew, err := InitDb("testMDb", "testM2BckupDb.db", false)
	if err != nil {t.Errorf("error -- InitDb: %v", err)}

	if  db.Entries!= dbnew.Entries {t.Errorf("error entries do not match kv: %d kvnew: %d", db.Entries, dbnew.Entries)}

	for key, val := range db.KV {

		valnew, err := dbnew.GetVal(key)
		if err!= nil {t.Errorf("error getval: %v", err)}
		if valnew != val {t.Errorf("error -- no value match for key: %s", key)}
	}

	db.CloseDb()
	dbnew.CloseDb()
}



func BenchmarkGet100(b *testing.B) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

//	os.RemoveAll("testMDb")

	numEntries := 100
	db, err := InitDb("testMDb", "testMDb100.db", false)
	if err != nil {log.Fatalf("error -- InitDb: %v", err)}

	db.Clean()

    err = db.FillRan(numEntries)
    if err != nil {log.Fatalf("error -- FillRan: %v", err)}

//    err = kv.Backup("testDbNew_Backup.dat")
//    if err != nil {log.Fatalf("error -- Backup: %v", err)}
	keyList := make([]string, len(db.KV))
	valList := make([]string, len(db.KV))
//	log.Printf("db size: %d\n", len(db.KV))
	cnt:=0
	for k, v := range(db.KV) {
		keyList[cnt] = k
		valList[cnt] = v
		cnt++
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		kidx := seededRand.Intn(numEntries)
		keyStr := keyList[kidx]
		valstr, err := db.GetVal(keyStr)
		if err != nil {log.Fatalf("GetVal %s: %v", keyStr, err)}
		if valstr != valList[kidx]	{log.Fatalf("values do not agree: %s is not %s!",valstr, valList[kidx])}

	}
}


func BenchmarkGet200(b *testing.B) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

//	os.RemoveAll("testDbNew")

	numEntries := 200
	db, err := InitDb("testMDb", "testMDb200.db", false)
	if err != nil {log.Fatalf("error -- InitDb: %v", err)}

	db.Clean()

    err = db.FillRan(numEntries)
    if err != nil {log.Fatalf("error -- FillRan: %v", err)}

	keyList := make([]string, len(db.KV))
	valList := make([]string, len(db.KV))

	cnt:=0
	for k, v := range(db.KV) {
		keyList[cnt] = k
		valList[cnt] = v
		cnt++
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		kidx := seededRand.Intn(numEntries)
		keyStr := keyList[kidx]
		valstr,err := db.GetVal(keyStr)
		if err != nil {log.Fatalf("GetVal %s: %v", keyStr, err)}
		if valstr != valList[kidx]	{log.Fatalf("values do not agree: %s is not %s!",valstr, valList[kidx])}
	}
}

func BenchmarkGet500(b *testing.B) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

//	os.RemoveAll("testDbNew")

	numEntries := 500
	db, err := InitDb("testMDb", "testMDb500.db", false)
	if err != nil {log.Fatalf("error -- InitDb: %v", err)}

	db.Clean()

    err = db.FillRan(numEntries)
    if err != nil {log.Fatalf("error -- FillRan: %v", err)}

	keyList := make([]string, len(db.KV))
	valList := make([]string, len(db.KV))

	cnt:=0
	for k, v := range(db.KV) {
		keyList[cnt] = k
		valList[cnt] = v
		cnt++
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		kidx := seededRand.Intn(numEntries)
		keyStr := keyList[kidx]
		valstr,err := db.GetVal(keyStr)
		if err != nil {log.Fatalf("GetVal %s: %v", keyStr, err)}
		if valstr != valList[kidx]	{log.Fatalf("values do not agree: %s is not %s!",valstr, valList[kidx])}
	}
}

func BenchmarkGet1000(b *testing.B) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

//	os.RemoveAll("testDbNew")

	numEntries := 1000
	db, err := InitDb("testMDb", "testMDb1000.db", false)
	if err != nil {log.Fatalf("error -- InitDb: %v", err)}

	db.Clean()

    err = db.FillRan(numEntries)
    if err != nil {log.Fatalf("error -- FillRan: %v", err)}

	keyList := make([]string, len(db.KV))
	valList := make([]string, len(db.KV))

	cnt:=0
	for k, v := range(db.KV) {
		keyList[cnt] = k
		valList[cnt] = v
		cnt++
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		kidx := seededRand.Intn(numEntries)
		keyStr := keyList[kidx]
		valstr, err := db.GetVal(keyStr)
		if err != nil {log.Fatalf("GetVal %s: %v", keyStr, err)}
		if valstr != valList[kidx]	{log.Fatalf("values do not agree: %s is not %s!",valstr, valList[kidx])}

	}
}

