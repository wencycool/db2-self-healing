package main

import (
	"fmt"
	"reflect"
)

func main() {
	d := new(D)
	n := reflect.TypeOf(d).Elem().NumField()
	for i := 0; i < n; i++ {
		fmt.Println(reflect.TypeOf(d.T).Elem().Field(i).Name)
	}
	f, _ := reflect.TypeOf(d).Elem().FieldByName("Name")
	fmt.Println("打印D中的name:", f.Name)
	fmt.Println(reflect.TypeOf)
}

type D struct {
	T
}

type T struct {
	Name string
	Age  int
}
