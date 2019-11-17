package main

import "fmt"

func main() {
	m := make([]*S, 0)
	m = append(m, &S{1})
	m = append(m, &S{2})
	m = append(m, &S{3})
	S := SList{}
	S = m
	S.print()
}

type S struct {
	Id int
}
type SList []*S

func (s SList) print() {
	for _, v := range s {
		fmt.Println(v.Id)
	}
	fmt.Println(len(s))
}
