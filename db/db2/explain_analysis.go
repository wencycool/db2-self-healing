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

//获取执行计划涉及到的对象信息
func (m *MonGetExplain) GetObjs() []*MonGetExplainObj {
	return m.objs
}
