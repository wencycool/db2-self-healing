package main

import (
	"fmt"
	"os"
	"reflect"
)

func main() {
	finfo, _ := os.Stat("/")
	r := reflect.ValueOf(finfo.Sys()).Elem().FieldByName("Dev").Uint()
	fmt.Println(r)
	r = reflect.ValueOf(finfo.Sys()).Elem().FieldByName("Ino").Uint()
	fmt.Println(r)
}
