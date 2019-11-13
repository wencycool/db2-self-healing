package main

import (
	"fmt"
	"reflect"
)

//测试返回接口

func main() {
	t := new(T)
	t.Id = 10
	p := reflect.ValueOf(t).Elem().FieldByName("Id1").CanAddr()
	fmt.Println(p)
}

type T struct {
	Id int
}
