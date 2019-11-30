package db2

//进行explain经验性分析

//是否有hashJoin
func (m *MonGetExplain) HasHSJoin() bool {
	return m.planNode.hasHSJoin()
}

//NLJoin的右子树是否存在IXAND操作
func (m *MonGetExplain) HasRightOperatorIXAnd() bool {
	return m.planNode.hasRightOperatorIXAnd()
}

//NLJoin的右子树是否是TabScan操作
func (m *MonGetExplain) HasRightOperatorTabScan() bool {
	return m.planNode.hasRightOperatorTabScan()
}

//是否存在索引的Sargeable扫描
func (m *MonGetExplain) HasIdxSargePredicate() bool {
	return m.planNode.hasIdxSargePredicate(m.predicates)
}

//预计产生多少行读
func (m *MonGetExplain) PredicateRowsScan() int {
	return m.planNode.predicateRowsScan()
}

//预计最大rows fetched
func (m *MonGetExplain) PredicateMaxRowsFetched() int {
	return m.planNode.predicateMaxRowsFetched()
}

//执行计划预估产生结果集
func (m *MonGetExplain) PredicateRowsFetched() int {
	return m.planNode.predicateRowsFetched()
}

//获取执行计划涉及到的对象信息
func (m *MonGetExplain) GetObjs() []*MonGetExplainObj {
	return m.objs
}

func (m *MonGetExplain) PrintData() {
	m.planNode.printData()
}
