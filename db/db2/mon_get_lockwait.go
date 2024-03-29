package db2

import (
	"bytes"
	"os/exec"
	"strings"
	"time"
)

//记录当前数据库锁等待信息，如果锁等待信息过多，那么最多记录1万行
//关于锁部分要解决两种情况，1 是否发生频繁短暂的锁等待情况，且数量较多；2 是否发生长时间锁等待情况；
//如果锁等待较多，那么说明锁问题较为严重应该予以处理

//记录锁等待信息
type MonGetLockWait struct {
	MataData
	SnapTime          time.Time `column:"CURRENT TIMESTAMP"`
	LockWaitStartTime time.Time `column:"LOCK_WAIT_START_TIME"`
	LockObjType       string    `column:"LOCK_OBJECT_TYPE"`
	LockMode          string    `column:"LOCK_MODE"`
	LockModeReq       string    `column:"LOCK_MODE_REQUESTED"`
	LockStatus        string    `column:"LOCK_STATUS"`
	LockEscalation    string    `column:"LOCK_ESCALATION"` //锁升级
	LockCnt           int       `column:"LOCK_COUNT"`
	TbspId            int64     `column:"TBSP_ID"`
	ReqAppHandle      int64     `column:"REQ_APPLICATION_HANDLE"`
	ReqAgentTid       int64     `column:"REQ_AGENT_TID"`
	ReqHexId          string    `column:"REQ_EXECUTABLE_ID"`
	HldAppHandle      int64     `column:"HLD_APPLICATION_HANDLE"` //0 代表该事务因崩溃恢复后正在执行回滚或者不一致事务，null代表无法找到该app
}

func NewMonGetLockWait() *MonGetLockWait {
	m := new(MonGetLockWait)
	m.rep = mon_get_rep
	m.tabname = "TABLE(MON_GET_APPL_LOCKWAIT(NULL, -1))"
	m.start_flag = m.tabname + mon_get_start_flag
	m.end_flag = m.tabname + mon_get_end_flag
	return m
}

func (m *MonGetLockWait) GetSqlText() string {
	return genSql(m)
}

func GetMonGetLockWaitList() ([]*MonGetLockWait, error) {
	m := NewMonGetLockWait()
	argSql := m.GetSqlText()
	cmd := exec.Command("db2", "+p", "-x", "-t")
	var in bytes.Buffer
	cmd.Stdin = &in
	log.Debug(argSql)
	in.WriteString(argSql)
	bs, err := cmd.CombinedOutput()
	result := string(bs)
	return getMonGetLockWaitListFromStr(result), err
}

//通过从数据库返回的结果，生成结果集
func getMonGetLockWaitListFromStr(str string) []*MonGetLockWait {
	m := NewMonGetLockWait()
	ms := make([]*MonGetLockWait, 0)
	start := strings.Index(str, m.start_flag) + len(m.start_flag)
	stop := strings.Index(str, m.end_flag)
	for _, line := range strings.Split(str[start:stop], "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := NewMonGetLockWait()
		d.tabname = ""
		d.start_flag = ""
		d.end_flag = ""
		if err := renderStruct(d, line); err != nil {
			continue
		}
		ms = append(ms, d)
	}
	return ms
}

//从MonGetLockWait list中查找锁定等待的源头，mon_get_app_lockwait表函数本身不提供锁源头的功能
func GetLockHeaderMap(lockwait []*MonGetLockWait) map[int64]int64 {
	headerMap := make(map[int64]int64, 0)
	tmp_map := make(map[int64]int64) //存放当前锁等待的handle和被等待的handle
	for _, m := range lockwait {
		tmp_map[m.ReqAppHandle] = m.HldAppHandle
	}
	for _, m := range lockwait {
		if _, ok := tmp_map[m.HldAppHandle]; !ok {
			headerMap[m.HldAppHandle] = m.HldAppHandle
		}
	}
	return headerMap
}
