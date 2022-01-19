package main

import (
	"os"

	"github.com/pedidopago/protodb/grpce"
)

func main() {
	if len(os.Args) != 2 {
		println("Usage: errdecode $ERROR_STRING")
		os.Exit(1)
	}
	println(grpce.DecodeString(os.Args[1]))
}
