package main

import (
	"fmt"
	"my/db/db2"
)

func main() {
	db2.ConnectDB("sample")
	result := db2.GetConnDistribByClientHostName()
	fmt.Println("连接数分布信息:")
	for _, v := range result {
		fmt.Printf("主机名:%-10s,连接数:%-5s\n", v[0], v[1])
	}
	result = db2.GetUowConnDistribByClientHostName()
	fmt.Println("活动连接数分布信息:")
	for _, v := range result {
		fmt.Printf("主机名:%-10s,连接数:%-5s\n", v[0], v[1])
	}
}
