// azulkv v7.0
// library of simple kv
// Author: prr azulsoftware
// Date: 27. Aug 2023
// copyright 2027 prr azul software
//
// V7: V6 plus mph implementation
//

package azulkv7

import (
	"fmt"
	"log"
	"math/rand"
	"time"
	"os"
	"bytes"
	"sync"
	"unsafe"
//	"sync/atomic"
//	"sort"

	"github.com/cespare/mph"
)

type DbObj struct {
	DirPath string
	Dbg bool
	TabNam string
	Tab *os.File
	mut *sync.RWMutex
	Entries int
	Cap int
	Keys []string
	Vals []string
	HTab *mph.Table
}


func InitDb(dirPath, tbNam string, dbg bool) (dbpt *DbObj, err error){

	if len(dirPath) == 0 {return nil, fmt.Errorf("no dirPath")}
	if len(tbNam) == 0 {return nil, fmt.Errorf("no tbNam")}

	var mutex sync.RWMutex

	db := DbObj {
		Dbg: dbg,
		Entries: 0,
		Cap: 1500,
		mut: &mutex,
	}

	db.Keys = make([]string, db.Cap)
	db.Vals = make([]string, db.Cap)

    // find dir
	tabPath := dirPath + "/" + tbNam

    _, err = os.Stat(dirPath)
    if err != nil {
        if os.IsNotExist(err) {
//            if dbg {log.Printf("db dir does not exist!\n")}
            log.Printf("db dir does not exist!\n")

            //create directory
           	if  err1 := os.Mkdir(dirPath, 0755); err1 != nil {return nil, fmt.Errorf("could not create dir: %v", err1)}

        } else {
            return nil, fmt.Errorf("could not open dir: %v", err)
        }
    }

//	log.Println("checking db file!")
    _, err = os.Stat(tabPath)
    if err != nil {
        if os.IsNotExist(err) {
            //create files
			log.Printf("creating new db: %s\n",tabPath)
            outfil, err1:= os.Create(tabPath)
            if err1 != nil {return nil, fmt.Errorf("could not create table: %v", err1)}
            db.Tab=outfil
            db.TabNam = tbNam
			initData := make([]byte,4)
			_, err = outfil.Write(initData[:])
			if err !=nil {return nil, fmt.Errorf("init write: %v", err)}

			outfil.Close()

		    db.DirPath = dirPath
    		db.TabNam = tbNam
			return &db, nil

        } else {
            return nil, fmt.Errorf("could not open db file: %v", err)
        }
	}

    db.DirPath = dirPath
    db.TabNam = tbNam
	dbp := &db

	err = dbp.Load(tbNam)
	if err != nil {return nil, fmt.Errorf("could not load table %s: %v", dbp.TabNam, err)}

	return dbp, nil
}

func (db *DbObj) CloseDb () (err error){

	tabnam := db.TabNam

	if len(tabnam) == 0 {tabnam = "dbClose.dat"}

	err = db.Backup(tabnam)
	return err
}

func GenRanData (rangeStart, rangeEnd int) (bdat []byte) {

	var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

    offset := rangeEnd - rangeStart

    randLength := seededRand.Intn(offset) + rangeStart
    bdat = make([]byte, randLength)

    charset := "abcdefghijklmnopqrstuvw0123456789"
    for i := range bdat {
        bdat[i] = charset[seededRand.Intn(len(charset)-1)]
    }
	return bdat
}


func (db *DbObj) FillRan (level int) (err error){

    (*db).mut.Lock()
    defer (*db).mut.Unlock()
	for i:=0; i<level; i++ {
		bdat := GenRanData(5, 25)
		valdat := GenRanData(5, 40)
		valstr := fmt.Sprintf("val-%d_%s",i,string(valdat))
//		valb := []byte(valstr)
		db.Keys[i] = string(bdat)
		db.Vals[i] = valstr
//		fmt.Printf(" %d: %d %s %s\n", i, (*db.Hash)[i], (*db.Keys)[i], (*db.Vals)[i])
	}
	db.Entries = level
//fmt.Printf("fil db: %v\n", dbpt)

	(*db).HTab = mph.Build(db.Keys[:db.Entries])
	return nil
}


func (db *DbObj) AddEntry (key, val string) (err error){

    (*db).mut.Lock()
    defer (*db).mut.Unlock()

	// case of no entries -> adding first entry
	if (*db).HTab == nil {
//log.Printf("add first entry!")
		db.Keys[0] = key
		db.Vals[0] = val
		db.Entries = 1

//log.Printf("db keys[%d]: %v\n", len(db.Keys),(*db).Keys[:db.Entries])

//log.Printf("building HTab!")
		(*db).HTab = mph.Build((*db).Keys[:db.Entries])
//log.Printf("built HTab!")

		return nil
	}
	idx, res := db.HTab.Lookup(key)
	if res {return fmt.Errorf("key exists")}

	incr :=1
	if len(db.Keys[idx]) == 0 {incr = 0}

	if db.Entries > db.Cap-100 {
		db.Keys = append(db.Keys, make([]string,100)...)
		db.Vals = append(db.Vals, make([]string,100)...)
		db.Cap += 100
	}

	nidx := db.Entries

	db.Keys[nidx] = key
	db.Vals[nidx] = val

	db.Entries += incr

	(*db).HTab = mph.Build(db.Keys[:db.Entries])

	return nil
}


func (db *DbObj) UpdEntry (key, val string) (idx int){

    (*db).mut.Lock()
    defer (*db).mut.Unlock()
	uidx, res := db.HTab.Lookup(key)

	if !res {return -1}

	idx = int(uidx)

	if len(db.Keys[idx]) == 0 {return -1}

	db.Vals[idx] = val

	return idx
}

func (db *DbObj) UpdEntryByIdx (idx int, val string) (err error){

	if idx < 0 || idx > db.Entries {return fmt.Errorf("invalid idx!")}

    (*db).mut.Lock()
    defer (*db).mut.Unlock()

	if len(db.Keys[idx]) == 0 {return fmt.Errorf("no key!")}

	db.Vals[idx] = val

	return nil
}

func (db *DbObj) DelEntry (key string) (err error){

    (*db).mut.Lock()
    defer (*db).mut.Unlock()
	uidx, res := db.HTab.Lookup(key)
	if !res {return fmt.Errorf("no key")}

	idx := int(uidx)

	if len(db.Keys[idx]) == 0 {return fmt.Errorf("deleted key!")}

	db.Keys[idx] = ""
	db.Vals[idx] = ""
	return nil
}

func (db *DbObj) DelEntryByIdx (idx int) (err error){

	if idx < 0 || idx > db.Entries {return fmt.Errorf("invalid idx!")}
    (*db).mut.Lock()
    defer (*db).mut.Unlock()

	if len(db.Keys[idx]) == 0 {return fmt.Errorf("deleted key!")}

	db.Keys[idx] = ""
	db.Vals[idx] = ""
	return nil
}


func (db *DbObj) GetVal (key string) (idx int, valstr string){

    (*db).mut.RLock()
    defer (*db).mut.RUnlock()
	uidx, res := db.HTab.Lookup(key)

	if !res {return -1, ""}

	idx = int(uidx)
	if len(db.Keys[idx]) == 0 {return -1, ""}

	valstr = db.Vals[idx]

	return idx, valstr

}

func (db *DbObj) GetValByIdx (idx int) (valstr string, err error){

	if idx < 0 || idx > db.Entries {return "", fmt.Errorf("invalid idx!")}
    (*db).mut.RLock()
    defer (*db).mut.RUnlock()

	if len(db.Keys[idx]) == 0 {return "", fmt.Errorf("no key for idx!")}

	valstr = db.Vals[idx]

	return valstr, nil

}



func (db *DbObj) FindKey (key string) (idx int) {

    (*db).mut.RLock()
    defer (*db).mut.RUnlock()
	uidx, res := db.HTab.Lookup(key)

	if !res {return -1}

	return int(uidx)
}


func (db *DbObj) Clean () {

    (*db).mut.Lock()
    defer (*db).mut.Unlock()
	for i:=0; i< db.Entries; i++ {
		db.Keys[i] = ""
		db.Vals[i] = ""
	}
	db.Entries = 0

	db.HTab = nil
	return
}


func (db *DbObj) Backup (tabNam string) (err error){


    (*db).mut.RLock()
    defer (*db).mut.RUnlock()
	numEntries := db.Entries
	dirPath := db.DirPath
//	log.Printf("backup dirPath: %s\n", dirPath)

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
		err = os.Rename(filPath, bfilPath)
    	if err != nil {return fmt.Errorf("rename backup file: %v", err)}
    }

//	numActEntries := numEntries
	numEnt := uint32(numEntries)
	numActEnt := uint32(numEntries)
	backSize := 8 + int(unsafe.Sizeof(numEnt))*numEntries *2

//  needs examination reg blocksize use!
// fix problem: need to add size dynamically
	bck := make([]byte, backSize, 4096*2)


	pt := (*[4]byte)(unsafe.Pointer(&numEnt))[:]
	copy(bck[:4], pt)

//	for i:=0; i<3; i++ {fmt.Printf("%d:", bck[i])}
//	fmt.Printf("%d\n", bck[4])

	start := 8
	for i:=0; i<db.Entries; i++ {
		entry := uint32(i)
		pt := (*[4]byte)(unsafe.Pointer(&entry))[:]
		copy(bck[start+i*4:start+(i+1)*4], pt)
	}

	start = db.Entries*4 + 8
	for i:=0; i<db.Entries; i++ {
		key := []byte(db.Keys[i])
		if len(key)==0 {
			numActEnt--
			continue
		}
		klen := uint16(len(db.Keys[i]))
		pt := (*[2]byte)(unsafe.Pointer(&klen))[:]
		copy(bck[start:start+2], pt)
		vlen := uint16(len(db.Vals[i]))
		pt2 := (*[2]byte)(unsafe.Pointer(&vlen))[:]
		copy(bck[start+2:start+4], pt2)
		start = start + 4
//		fmt.Printf("  %d: kl %d vl %d\n",i, klen, vlen)

		copy(bck[start:start+int(klen)],key)
		val := []byte(db.Vals[i])
		copy(bck[start +int(klen):start+int(klen)+int(vlen)],val)
//		fmt.Printf("klen: %d vlen: %d key: %s val %s\n", klen, vlen, string(key), string(val))
		start = start + int(klen) + int(vlen)
// increase slice if start > max
		if (start + 512)> cap(bck) {
			addBlock := (start+ 4096*2 - cap(bck))/4096
//			log.Printf("cap: %d size: %d\n", cap(bck), start)
			bck = append(bck, make([]byte, addBlock*4096)...)
		}
	}
	endpt := start
	pt2 := (*[4]byte)(unsafe.Pointer(&numActEnt))[:]
	copy(bck[4:8], pt2)


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


func (db *DbObj) Load(tabNam string) (err error){
	var numEntries uint32
	var numActEntries uint32

//	capacity := db.Cap
    (*db).mut.Lock()
    defer (*db).mut.Unlock()

	dirPath := db.DirPath
	filPath := dirPath + "/" + tabNam
//	log.Printf("load: %s\n", filPath)

	bckup, err := os.ReadFile(filPath)
	if err != nil {return fmt.Errorf("could not read table: %v", err)}

	siz := len(bckup)

//	fmt.Printf("backup: %d\n",siz)

	if siz < 4 {return fmt.Errorf("no valid numEntries found!")}

	numEntries = *(*uint32)(unsafe.Pointer(&bckup[0]))

	// no need to read keys if there are no entries
	if numEntries == 0 {return nil}

	numActEntries = *(*uint32)(unsafe.Pointer(&bckup[4]))

	numKeys := int(numActEntries)

	db.Entries = numKeys

	entries := make([]uint32, int(numEntries))

	for i:=0; i< len(entries); i++ {
		entries[i] = *(*uint32)(unsafe.Pointer(&bckup[8+i*4]))
	}

	start := 8 + len(entries)*4
	if numKeys > db.Cap - 100 {

		incr := numKeys +100 -db.Cap
		db.Keys = append(db.Keys, make([]string,incr)...)
		db.Vals = append(db.Vals, make([]string,incr)...)
		db.Cap = cap(db.Keys)
	}

	for i:=0; i< numKeys; i++ {
		klen := *(*uint16)(unsafe.Pointer(&bckup[start]))
		vlen := *(*uint16)(unsafe.Pointer(&bckup[start +2]))
//		fmt.Printf("  %d: klen %d vlen %d\n", i, klen, vlen)
		key := bckup[start +4: start+4+int(klen)]
		val := bckup[start +4 + int(klen): start+4+int(klen)+int(vlen)]
		start = start + 4 + int(klen) + int(vlen)
		db.Keys[i] = string(key)
		db.Vals[i] =string(val)

//		fmt.Printf("klen: %d vlen: %d key: %s val %s\n", klen, vlen, string(key), string(val))
	}

	(*db).HTab = mph.Build(db.Keys[:db.Entries])

	return nil
}


func (db *DbObj) PrintDb (idx int, num int) {

    fmt.Printf("************ AzulDb *********\n")
    fmt.Printf("Dir:    %s\n",db.DirPath)
    fmt.Printf("Table:  %s\n",db.TabNam)
    fmt.Printf("********* End AzulKV *******\n")

	fmt.Printf("********* Entries: %d *************\n", (db.Entries))
	end := idx+num
	if end > db.Entries {
		fmt.Printf("invalid idx; idx + num [%d] > entires: %d!\n", idx+num, db.Entries)
		end = db.Entries
	}
	fmt.Println("  i  Idx     Key   Value")
	for i:=idx; i<end; i++ {
		fmt.Printf("  [%2d]:  %20s %s\n", i, db.Keys[i], db.Vals[i])
	}
	fmt.Printf("********* End Entries *************\n")
	return
}

func PrintDB (db *DbObj) {

    fmt.Printf("************ AzulDb *********\n")
    fmt.Printf("Dir:    %s\n",db.DirPath)
    fmt.Printf("Table:  %s\n",db.TabNam)
    fmt.Printf("********* End AzulKV *******\n")

	fmt.Printf("********* Entries: %d *************\n", (db.Entries))
	fmt.Println("  i  Idx      Key   Value")
	for i:=0; i<db.Entries; i++ {
		fmt.Printf("  [%2d]: %20s %s\n", i, db.Keys[i], db.Vals[i])
	}
	fmt.Printf("********* End Entries *************\n")
	return
}

func PrintDbFil(filPath string) (err error){
	var numEntries uint32
	var numActEntries uint32

	bckup, err := os.ReadFile(filPath)
	if err != nil {return fmt.Errorf("could not read table: %v", err)}

	siz := len(bckup)

	fmt.Printf("file size: %d\n",siz)

	if siz < 4 {return fmt.Errorf("no valid numEntries found!")}

	numEntries = *(*uint32)(unsafe.Pointer(&bckup[0]))

	fmt.Printf("entries: %d\n", numEntries)

	// no need to read keys if there are no entries
	if numEntries == 0 {return nil}

	numActEntries = *(*uint32)(unsafe.Pointer(&bckup[4]))

	numKeys := int(numActEntries)

	fmt.Printf("keys: %d\n", numKeys)

//	db.Entries = numKeys

	entries := make([]uint32, int(numEntries))

	for i:=0; i< len(entries); i++ {
		entries[i] = *(*uint32)(unsafe.Pointer(&bckup[8+i*4]))
	}

	start := 8 + len(entries)*4

	for i:=0; i< numKeys; i++ {
		klen := *(*uint16)(unsafe.Pointer(&bckup[start]))
		vlen := *(*uint16)(unsafe.Pointer(&bckup[start +2]))
		fmt.Printf("  %d: klen %d vlen %d\n", i, klen, vlen)
		key := bckup[start +4: start+4+int(klen)]
		val := bckup[start +4 + int(klen): start+4+int(klen)+int(vlen)]
		start = start + 4 + int(klen) + int(vlen)

		fmt.Printf("klen: %d vlen: %d key: %s val %s\n", klen, vlen, string(key), string(val))
	}

	return nil
}
