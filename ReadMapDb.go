// ReadDbFil
// program that read the DbFile for examination
//
// Author: prr, azul software
// Date: 23. Sept 2023
// copyright (c) 2023 prr, axul software
//

package main


import (
	"os"
	"log"
	"fmt"
	azuldb "db/azulkv2/azulkvLibMap"
)

func main() {

	numargs := len(os.Args)

	if numargs <2 {fmt.Println("insufficient arguments: usage is: ReadDb dbfile!"); os.Exit(1);}

	if os.Args[1] == "help" {
		fmt.Println("usage is: ReadDb dbfile")
		os.Exit(1)
	}

	infil := os.Args[1]
	err := azuldb.PrintDbFil(infil)
	if err != nil {
		log.Fatalf("error -- PrintDbFil: %v\n",err)
	}
}
