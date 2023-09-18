// azulkv v3.0
// library of simple kv
// Author: prr azulsoftware
// Date: 27. Aug 2023
// copyright 2027 prr azul software
//
// v3:
// add mutexes
//

package azulkv3

import (
	"fmt"
	"log"
	"math/rand"
	"time"
	"os"
	"bytes"
	"sync"
	"unsafe"
//	"sort"

	"github.com/dgryski/go-t1ha"
)

type DbObj struct {
	DirPath string
	Dbg bool
	TabNam string
	Tab *os.File
	Db *DB
}

type DB struct {
	mDb sync.RWMutex
	Entries int
	Cap int
	HashList []hash
	Keys []string
	Vals []string
}

type hash struct {
	Hash uint64
	Idx int
}

func InitDb(dirPath, tbNam string, dbg bool) (dbpt *DbObj, err error){

	if len(dirPath) == 0 {return nil, fmt.Errorf("no dirPath")}
	if len(tbNam) == 0 {return nil, fmt.Errorf("no tbNam")}

	db := DB {
		Cap: 500,
	}

	dbobj := DbObj {
		Dbg: dbg,
		Db: &db,
	}


	db.Entries = 0
	capacity := db.Cap
	db.HashList = make([]hash,0, capacity)
	db.Keys = make([]string, 0, capacity)
	db.Vals = make([]string,0, capacity)

    // find dir
    _, err = os.Stat(dirPath)
	tabPath := dirPath + "/" + tbNam
    if err != nil {
        if os.IsNotExist(err) {
            if dbg {log.Printf("db dir does not exist!\n")}

            //create directory
           	if  err1 := os.Mkdir(dirPath, 0755); err1 != nil {return nil, fmt.Errorf("could not create dir: %v", err1)}

            dbobj.DirPath = dirPath

            //create files

            outfil, err1:= os.Create(tabPath)
			defer outfil.Close()
            if err1 != nil {return nil, fmt.Errorf("could not create table: %v", err1)}
            dbobj.Tab=outfil
            dbobj.TabNam = tbNam

			initData := make([]byte,4)
			_, err = outfil.Write(initData[:])
			if err !=nil {return nil, fmt.Errorf("init write: %v", err)}
			return &dbobj, nil
        } else {
            return nil, fmt.Errorf("could not open dir: %v", err)
        }
    }
    if dbg {log.Printf("azulkv dir exists!\n")}

    dbobj.DirPath = dirPath
    dbobj.TabNam = tbNam

	dbp := &dbobj

	err = dbp.Load(tbNam)
	if err != nil {return nil, fmt.Errorf("could not load table %s: %v", dbobj.TabNam, err)}

	return dbp, nil
}

func (dbpt *DbObj) CloseDb () (err error){

	tabnam := dbpt.TabNam
	if len(tabnam) == 0 {tabnam = "dbClose.dat"}
	err = dbpt.Backup(tabnam)
	return err
}

func GetHash(bdat []byte) (hash uint64) {

	seed :=uint64(0)
	hash = t1ha.Sum64(bdat, seed)

	return hash
}

func GenRanData (rangeStart, rangeEnd int) (bdat []byte) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

//    rangeStart := 5
//    rangeEnd := 25
    offset := rangeEnd - rangeStart

    randLength := seededRand.Intn(offset) + rangeStart
    bdat = make([]byte, randLength)

    charset := "abcdefghijklmnopqrstuvw0123456789"
    for i := range bdat {
        bdat[i] = charset[seededRand.Intn(len(charset)-1)]
    }
	return bdat
}

/*
func (dbpt *DbObj) FillRan (level int) (err error){

	db := (*dbpt).Db
	h := hash {}
	for i:=0; i<level; i++ {
		bdat := GenRanData(5, 25)
		hashval := GetHash(bdat)
		valdat := GenRanData(5, 40)
		valstr := fmt.Sprintf("val-%d_%s",i,string(valdat))
//		valb := []byte(valstr)
		*db.Keys = append(*db.Keys,string(bdat))
		h.Idx = i
		h.Hash = hashval
		*db.HashList = append(*db.HashList, h)
		*db.Vals = append(*db.Vals,valstr)
//		fmt.Printf(" %d: %d %s %s\n", i, (*db.Hash)[i], (*db.Keys)[i], (*db.Vals)[i])
	}
	(*db.Entries) = level
	dbpt = &db
//fmt.Printf("fil db: %v\n", dbpt)
	return nil
}


func (dbp *DbObj) AddEntry (key, val string) (err error){

	db := *dbp
	idx := (*db.Entries)
	if idx > db.Cap-2 {return fmt.Errorf("entry exceeds limits")}

	hashval := GetHash([]byte(key))
	hashdat := hash {
			Hash: hashval,
			Idx: idx,
	}
	*db.HashList = append(*db.HashList, hashdat)

//	(*db.HashList)[idx].Hash = hashval
//	(*db.HashList)[idx].Idx = idx

	*db.Keys = append(*db.Keys, key)
	*db.Vals = append(*db.Vals, val)
//	(*db.Keys)[idx] = key
//	(*db.Vals)[idx] = val

	(*db.Entries)++
	dbp = &db

	return nil
}

func (dbp *DbObj) UpdEntry (key, val string) (idx int){
	db := *dbp
	for i:=0; i< (*db.Entries); i++ {
		if (*db.Keys)[i] == key {
			idx = i
			(*db.Vals)[i] = val
			dbp = &db
			return idx
		}
	}
	return -1
}

func (dbp *DbObj) UpdEntryByIdx (idx int, val string) (err error){

	db := *dbp
	if idx < 0 ||idx > (*db.Entries) {return fmt.Errorf("invalid index")}
	(*db.Vals)[idx] = val
	dbp = &db
	return nil
}

func (dbp *DbObj) DelEntry (idx int) (err error){

	db := *dbp
	if idx > db.Cap {return fmt.Errorf("invalid index")}
	(*db.HashList)[idx].Hash = 0
	(*db.HashList)[idx].Idx = 0
	(*db.Keys)[idx] = ""
	(*db.Vals)[idx] = ""
	dbp = &db

	return nil
}

func (dbp *DbObj) GetVal (keyStr string) (idx int, valstr string){

	db := *dbp
	idx = -1
	for i:=0; i< (*db.Entries); i++ {
		if (*db.Keys)[i] == keyStr {
			idx = i
			valstr = (*db.Vals)[i]
			return idx, valstr
		}
	}
	return idx, ""
}

func (dbp *DbObj) GetValByIdx (idx int)(valstr string, err error){

	db := *dbp
	if idx < 0 || idx > (*db.Entries) {return "", fmt.Errorf("not a valid index!")}
	valstr = (*db.Vals)[idx]
	return valstr, nil
}

func (dbp *DbObj) GetValByHash (hash uint64) (idx int, valstr string){

	db := *dbp
//	hashval := GetHash([]byte(key))

	for i:=0; i< (*db.Entries); i++ {
		if (*db.HashList)[i].Hash == hash {
			idx = i
			valstr = (*db.Vals)[i]
			return idx, valstr
		}
	}
	return idx, ""
}

func (dbp *DbObj) FindKeyByHash (key string) (idx int){
	db := *dbp
	hashval := GetHash([]byte(key))

	for i:=0; i< (*db.Entries); i++ {
		if (*db.HashList)[i].Hash == hashval {
			idx = i
			return idx
		}
	}
	return -1
}

func (dbp *DbObj) FindKey (keyStr string) (idx int) {

	db := *dbp
	for i:=0; i< (*db.Entries); i++ {
		if (*db.Keys)[i] == keyStr {
			idx = i
			return idx
		}
	}
	return -1

}

func (dbp *DbObj) GetKeyByIdx (idx int) (key string) {

	db := *dbp
	if idx > (*db.Entries) {return ""}

	key = (*db.Keys)[idx]
	return key
}

func (dbP *DbObj) Clean () (err error){

	return err
}
*/

func (dbp *DbObj) Backup (tabNam string) (err error){

    dbobj := *dbp
	db := *dbobj.Db
//    kvMap, err := InitKV("testDb", true)
//    if err != nil {log.Fatalf("error -- InitKV: %v", err)}
	numEntries := db.Entries
	dirPath := dbobj.DirPath

	if len(dirPath) == 0 {return fmt.Errorf("DirPath not found!")}

	oldBackup := ""
	oldIdx := bytes.IndexByte([]byte(tabNam), '.')
	if oldIdx == -1 {
		oldBackup = tabNam + ".old"
	} else {
		oldBackup = string(tabNam[:oldIdx]) + ".old"
	}

	// check whether a backup file exists
	// if it exists rename it as tmp file
	bfilPath := dirPath + "/" + oldBackup
	tlen := len(oldBackup) - 4
	tmpBackup := string(oldBackup[:tlen]) + ".tmp"
	tmpFilPath :=dirPath + "/" + tmpBackup
    _, err = os.Stat(bfilPath)
    if err == nil {
		// todo add timestamp to filnam if fil already exists!
		err = os.Rename(bfilPath, tmpFilPath)
    	if err != nil {return fmt.Errorf("rename old backup file: %v", err)}
    }

	// check whether db file with tabname exists
	// if it does, rename db file to tabname.old
	// now we have a 2 files: the old backup file and the existing file renamed
	filPath := dirPath + "/" + tabNam
    _, err = os.Stat(filPath)
    if err == nil {
		// todo add timestamp to filnam if fil already exists!
		err = os.Rename(filPath, string(oldBackup))
    	if err != nil {return fmt.Errorf("rename backup file: %v", err)}
    }

	numEnt := uint32(numEntries)
	backSize := 4 + int(unsafe.Sizeof(numEnt))*numEntries *2

//  needs examination reg blocksize use!
// fix problem: need to add size dynamically
	bck := make([]byte, backSize, 4096*2)


	pt := (*[4]byte)(unsafe.Pointer(&numEnt))[:]
	copy(bck[:4], pt)

//	for i:=0; i<3; i++ {fmt.Printf("%d:", bck[i])}
//	fmt.Printf("%d\n", bck[4])

	start := 4
	for i:=0; i<numEntries; i++ {
		entry := uint32(i)
		pt := (*[4]byte)(unsafe.Pointer(&entry))[:]
		copy(bck[start+i*4:start+(i+1)*4], pt)
	}

	start = numEntries*4 + 4
	for i:=0; i<numEntries; i++ {
		klen := uint16(len(db.Keys[i]))
		pt := (*[2]byte)(unsafe.Pointer(&klen))[:]
		copy(bck[start:start+2], pt)
		vlen := uint16(len(db.Vals[i]))
		pt2 := (*[2]byte)(unsafe.Pointer(&vlen))[:]
		copy(bck[start+2:start+4], pt2)
		start = start + 4
//		fmt.Printf("  %d: kl %d vl %d\n",i, klen, vlen)
		key := []byte(db.Keys[i])
		copy(bck[start:start+int(klen)],key)
		val := []byte(db.Vals[i])
		copy(bck[start +int(klen):start+int(klen)+int(vlen)],val)
//		fmt.Printf("klen: %d vlen: %d key: %s val %s\n", klen, vlen, string(key), string(val))
		start = start + int(klen) + int(vlen)
// increase slice if start > max
		if (start + 1000)> cap(bck) {
			log.Printf("cap: %d size: %d\n", cap(bck), start)
			bck = append(bck, make([]byte, 4096)...)
		}
	}
	endpt := start
//	fmt.Printf("endpt: %d\n",endpt)

	// we can now create a new file
	outfil, err:= os.Create(filPath)
	if err != nil {return fmt.Errorf("could not create table: %v", err)}

	_, err = outfil.Write(bck[:endpt])
	if err !=nil {return fmt.Errorf("backup write: %v", err)}

	err = outfil.Close()
	if err !=nil {return fmt.Errorf("closing db file: %v", err)}

    _, err = os.Stat(tmpFilPath)
	if err == nil {
		err = os.Remove(tmpFilPath)
		if err !=nil {return fmt.Errorf("removing old backup file: %v", err)}
	}

	return nil
}


func (dbp *DbObj) Load(tabNam string) (err error){
	var numEntries uint32

    dbobj := *dbp
	db := *dbobj.Db
//	capacity := db.Cap

	dirPath := dbobj.DirPath
	filPath := dirPath + "/" + tabNam
	log.Printf("load: %s\n", filPath)

	bckup, err := os.ReadFile(filPath)
	if err != nil {return fmt.Errorf("could not read table: %v", err)}

	siz := len(bckup)

//	fmt.Printf("backup: %d\n",siz)

	if siz < 4 {return fmt.Errorf("no valid numEntries found!")}

	numEntries = *(*uint32)(unsafe.Pointer(&bckup[0]))
	numKeys := int(numEntries)
	db.Entries = numKeys

	// no need to read keys if there are no entries
	if numKeys == 0 {
		dbobj.Db = &db
	    dbp = &dbobj
		return nil
	}

	entries := make([]uint32, numKeys)

	for i:=0; i< numKeys; i++ {
		entries[i] = *(*uint32)(unsafe.Pointer(&bckup[4+i*4]))
	}

	start := 4 + numKeys*4
	for i:=0; i< numKeys; i++ {
		klen := *(*uint16)(unsafe.Pointer(&bckup[start]))
		vlen := *(*uint16)(unsafe.Pointer(&bckup[start +2]))
//		fmt.Printf("  %d: klen %d vlen %d\n", i, klen, vlen)
		key := bckup[start +4: start+4+int(klen)]
		val := bckup[start +4 + int(klen): start+4+int(klen)+int(vlen)]
		start = start + 4 + int(klen) + int(vlen)
		db.Keys = append(db.Keys,string(key))
		db.Vals = append(db.Vals,string(val))
		h := hash {
			Hash: GetHash(key),
			Idx: i,
		}
		db.HashList = append(db.HashList, h)

//		fmt.Printf("klen: %d vlen: %d key: %s val %s\n", klen, vlen, string(key), string(val))
	}
    dbobj.Db = &db
	dbp = &dbobj
	return nil
}

/*
func (dbp *DbObj) SortHash(){

    db := *dbp
	num := (*db.Entries)
	hashList := (*db.HashList)[:num]
	for i:=0; i< len(hashList); i++ {
		fmt.Printf("%d hash: %d idx: %d\n", i,hashList[i].Hash, hashList[i].Idx) 
	}
	fmt.Println("***")
	sort.Slice(hashList, func(i, j int) bool {
		return hashList[i].Hash < hashList[j].Hash
	})

	for i:=0; i< len(hashList); i++ {
		fmt.Printf("%d hash: %d idx: %d\n", i,hashList[i].Hash, hashList[i].Idx) 
	}
	fmt.Println("***")

	dbp.HashList = &hashList
	dbp = &db
}

*/

func (dbpt *DbObj) PrintDb (idx int, num int) {

	db := (*dbpt).Db
    fmt.Printf("************ AzulDb *********\n")
    fmt.Printf("Dir:    %s\n",(*dbpt).DirPath)
    fmt.Printf("Table:  %s\n",(*dbpt).TabNam)
    fmt.Printf("********* End AzulKV *******\n")

	fmt.Printf("********* Entries: %d *************\n", (db.Entries))
	if idx+num > db.Entries {
		fmt.Printf("invalid idx; idx + num > %d!\n", db.Entries)
		return
	}
	fmt.Println("  i  Idx  Hash    Key   Value")
	for i:=idx; i<idx + num; i++ {
		fmt.Printf("  [%2d]: %d %20s %s\n", i, db.HashList[i].Hash, db.Keys[i], db.Vals[i])
	}
	fmt.Printf("********* End Entries *************\n")
	return
}

