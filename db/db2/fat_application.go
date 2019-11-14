package db2

//判断agent是否可以进行做force操作，主要包括是否大事务，是否包含reorg等DDL操作
type FatApplication struct {
	uow MonGetCurUowExtend
	act MonGetActStmt
}
