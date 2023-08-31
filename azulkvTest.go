package main


import (
	"fmt"
	"log"
//	"os"
	"db/azulkv2/azulkvLib"
)

func main() {

	kvMap, err := azulkv2.InitKV("testDb", true)
	if err != nil {log.Fatalf("error -- InitKV: %v", err)}

//	fmt.Printf("kvmap actual %d capacity: %d\n", kvMap.NumEntries, kvMap.Cap)
//	fmt.Printf("hash len: %d\n", len(*kvMap.Hash))


	kvMap.FillRan(5)

	azulkv2.PrintDb(kvMap)
	kvMap.PrintKV(0,5)

	kvMap.SortHash()
	kvMap.PrintKV(0,5)


	log.Printf("create backup!")
	err = kvMap.Backup("tabBackup.dat")
	if err != nil {log.Fatalf("error -- Backup: %v", err)}

	kvdb, err := azulkv2.InitKV("testDb", true)
	if err != nil {log.Fatalf("error -- InitKV: %v", err)}
	log.Printf("load Backup!")
	err = kvdb.Load("tabBackup.dat")
	if err != nil {log.Fatalf("error -- Load: %v", err)}

	azulkv2.PrintDb(kvdb)
	kvdb.PrintKV(0,5)

	fmt.Printf("success\n")

}

