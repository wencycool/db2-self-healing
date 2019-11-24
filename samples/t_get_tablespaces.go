package main

import (
	"fmt"
	filename "github.com/keepeye/logrus-filename"
	"github.com/sirupsen/logrus"
	"my/db/db2"
)

func init() {
	var log *logrus.Logger
	log = logrus.New()
	filenameHook := filename.NewHook()
	filenameHook.Field = "line"
	log.AddHook(filenameHook)
	log.SetLevel(logrus.TraceLevel)
	db2.LogRegister(log)
}
func main() {
	db2.ConnectDB("sample")
	tbsplist := db2.NewMonGetTbspList()
	for _, tbsp := range tbsplist {
		fmt.Println(tbsp.TbspName, tbsp.TbspId, tbsp.State, tbsp.PageSize, tbsp.ExtendSize)
	}
	containerlist := db2.NewMonGetContainerList()
	for _, c := range containerlist {
		fmt.Println(c.TbspName, c.TbspId, c.FsId, c.StripeSet, c.ContainName)
	}
}
