// azulkvLibSMap
// library of simple kv
// Author: prr azulsoftware
// Date: 27. Aug 2023
// copyright 2027 prr azul software
//
// based on azulkvLibV5
// replace slice with sync.map

package azulkvSMap

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

//	"github.com/dgryski/go-t1ha"
)

type DbObj struct {
	DirPath string
	Dbg bool
	TabNam string
	Tab *os.File
//	mut *sync.RWMutex
	Entries int
	Cap int
	KV  *(sync.Map)
}


func InitDb(dirPath, tbNam string, dbg bool) (dbpt *DbObj, err error){

	if len(dirPath) == 0 {return nil, fmt.Errorf("no dirPath")}
	if len(tbNam) == 0 {return nil, fmt.Errorf("no tbNam")}

	var kv sync.Map

	db := DbObj {
		Dbg: dbg,
		Entries: 0,
		Cap: 1500,
//		mut: &mutex,
		KV: &kv,
	}

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

	if db.Dbg {log.Println("checking db file!")}
    _, err = os.Stat(tabPath)
    if err != nil {
        if os.IsNotExist(err) {
            //create files
			if db.Dbg {log.Printf("creating new db: %s\n",tabPath)}
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

//log.Println("load")
	err = dbp.Load(tbNam)
	if err != nil {return nil, fmt.Errorf("could not load table %s: %v", dbp.TabNam, err)}
//log.Println("finished load")

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

	for i:=0; i<level; i++ {
		bdat := GenRanData(5, 25)
		valdat := GenRanData(5, 40)
		valstr := fmt.Sprintf("val-%d_%s",i,string(valdat))
		(db.KV).Store(string(bdat), valstr)
	}
	db.Entries = level
//fmt.Printf("fil db: %v\n", dbpt)
	return nil
}


func (db *DbObj) AddEntry (key, val string) (err error){

	_, ok := db.KV.Load(key)
	if ok {return fmt.Errorf("key exists!")}

	(db.KV).Store(key,val)
	db.Entries++

	return nil
}


func (db *DbObj) UpdEntry (key, val string) (err error){

	_, ok := (db.KV).Load(key)
	if !ok {return fmt.Errorf("key not valid!")}

	(db.KV).Store(key,val)
	return nil
}


func (db *DbObj) DelEntry (key string) (err error){

	db.KV.Delete(key)
	db.Entries--
	return nil
}


func (db *DbObj) GetVal (key string) (val string, err error){

	vald, ok := db.KV.Load(key)
	if !ok {return "", fmt.Errorf("key not valid!")}

	return vald.(string), nil
}

func (db *DbObj) FindKey (key string) (err error) {

	_, ok := db.KV.Load(key)
	if !ok {return fmt.Errorf("key not valid!")}
	return nil
}


func (db *DbObj) Clean () {
	(db.KV).Range(func(k, v any) bool {
		(db.KV).Delete(k)
		return true
	})
	db.Entries = 0
	return
}

func (db *DbObj) CompareDb (dbnew *DbObj) (err error){

	keyList := make([]string,20)
	valList := make([]string,20)
	cnt:=0
	(db.KV).Range(func(k, v any) bool {
		keyList[cnt] = k.(string)
		valList[cnt] = v.(string)
		cnt++
		return true
	})

	for i:=0; i< cnt; i++ {
		key := keyList[i]
		valnew, ok := (dbnew.KV).Load(key)
		if !ok {return fmt.Errorf("item %d: key %s does not exist", i, key)}
		if valList[i] != valnew.(string) {return fmt.Errorf("item %d: do not agree: %s != %s",i, valList[i], valnew.(string))}
	}
	return nil
}

func (db *DbObj) Backup (tabNam string) (err error){

	cnt:=0
	(db.KV).Range(func(k, v any) bool {
		cnt++
		return true
	})

	if db.Entries != cnt {log.Printf("Entries: %d  %d\n", db.Entries, cnt)}

	numEntries := cnt
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
	cnt = 0
	(db.KV).Range(func(k, v any) bool {
		entry := uint32(cnt)
		pt := (*[4]byte)(unsafe.Pointer(&entry))[:]
		copy(bck[start+cnt*4:start+(cnt+1)*4], pt)
		cnt++
		return true
	})
/*
	for i:=0; i<numEntries; i++ {
		entry := uint32(i)
		pt := (*[4]byte)(unsafe.Pointer(&entry))[:]
		copy(bck[start+i*4:start+(i+1)*4], pt)
	}
*/


	start = numEntries*4 + 4
//	cnt = 0
	(db.KV).Range(func(keyStr, valStr any) bool {
		klen := uint16(len(keyStr.(string)))
		pt := (*[2]byte)(unsafe.Pointer(&klen))[:]
		copy(bck[start:start+2], pt)
		vlen := uint16(len(valStr.(string)))
		pt2 := (*[2]byte)(unsafe.Pointer(&vlen))[:]
		copy(bck[start+2:start+4], pt2)
		start = start + 4
//		fmt.Printf("  %d: kl %d vl %d\n",i, klen, vlen)
		key := []byte(keyStr.(string))
		copy(bck[start:start+int(klen)],key)
		val := []byte(valStr.(string))
		copy(bck[start +int(klen):start+int(klen)+int(vlen)],val)
//		fmt.Printf("klen: %d vlen: %d key: %s val %s\n", klen, vlen, string(key), string(val))
		start = start + int(klen) + int(vlen)
// increase slice if start > max
		if (start + 1000)> cap(bck) {
//			log.Printf("cap: %d size: %d\n", cap(bck), start)
			bck = append(bck, make([]byte, 4096)...)
		}
		return true
	})

//	(*db).mut.RUnlock()
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


func (db *DbObj) Load(tabNam string) (err error){
	var numEntries uint32

//	capacity := db.Cap

	dirPath := db.DirPath
	filPath := dirPath + "/" + tabNam
//	log.Printf("load: %s\n", filPath)

	bckup, err := os.ReadFile(filPath)
	if err != nil {return fmt.Errorf("could not read table: %v", err)}

	siz := len(bckup)

//	fmt.Printf("backup: %d\n",siz)

	if siz < 4 {return fmt.Errorf("no valid numEntries found!")}

	numEntries = *(*uint32)(unsafe.Pointer(&bckup[0]))
	numKeys := int(numEntries)

//	(*db).mut.Lock()
//log.Println("load locking")
//	defer (*db).mut.Unlock()
	db.Entries = numKeys

	// no need to read keys if there are no entries
	if numKeys == 0 {
		return nil
	}

	entries := make([]uint32, numKeys)

	for i:=0; i< numKeys; i++ {
		entries[i] = *(*uint32)(unsafe.Pointer(&bckup[4+i*4]))
	}

	start := 4 + numKeys*4
	if numKeys > db.Cap -100 {
		// todo
		return fmt.Errorf("need to enlarge arrays!")
	}

	for i:=0; i< numKeys; i++ {
		klen := *(*uint16)(unsafe.Pointer(&bckup[start]))
		vlen := *(*uint16)(unsafe.Pointer(&bckup[start +2]))
//		fmt.Printf("  %d: klen %d vlen %d\n", i, klen, vlen)
		key := bckup[start +4: start+4+int(klen)]
		val := bckup[start +4 + int(klen): start+4+int(klen)+int(vlen)]
		start = start + 4 + int(klen) + int(vlen)
		(db.KV).Store(string(key), string(val))
//		fmt.Printf("klen: %d vlen: %d key: %s val %s\n", klen, vlen, string(key), string(val))
	}
	return nil
}

/*
func (db *DbObj) PrintDbAll (idx int, num int) {

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
	fmt.Println("  i  Idx  Hash    Key   Value")
	for i:=idx; i<end; i++ {
		fmt.Printf("  [%2d]: %d %20s %s\n", i, db.HashList[i].Hash, db.Keys[i], db.Vals[i])
	}
	fmt.Printf("********* End Entries *************\n")
	return
}
*/

func PrintDB (db *DbObj) {

    fmt.Printf("************ AzulDb *********\n")
    fmt.Printf("Dir:    %s\n",db.DirPath)
    fmt.Printf("Table:  %s\n",db.TabNam)
    fmt.Printf("********* End AzulKV *******\n")

	fmt.Printf("********* Entries: %d *************\n", (db.Entries))
	fmt.Println("  i  Key   Value")
	cnt :=0
	(db.KV).Range(func(keyStr, valStr any) bool {
		fmt.Printf("  [%2d]: %-20s %-45s\n", cnt, keyStr.(string), valStr.(string))
		cnt++
		return true
	})
	fmt.Printf("********* End Entries *************\n")
	return
}

