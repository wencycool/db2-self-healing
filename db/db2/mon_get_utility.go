package db2

import (
	"strings"
	"time"
)

type MonGetUtil struct {
	MataData
	SnapTime       time.Time `column:"CURRENT TIMESTAMP"`
	AppHandle      int64     `column:"APPLICATION_HANDLE"`
	AppId          string    `column:"APPLICATION_ID"`
	AppName        string    `column:"APPLICATION_NAME"`
	SessionId      string    `column:"SESSION_AUTH_ID"`        //记录连接数据库使用的用户
	ClientUser     string    `column:"CLIENT_USERID"`          //记录客户端登录用户
	ClientAppName  string    `column:"CLIENT_APPLNAME"`        //记录客户端发起的程序名称
	UtilId         int64     `column:"UTILITY_ID"`             //作业id，分区内唯一
	UtilStartTime  time.Time `column:"UTILITY_START_TIME"`     //作业开始时间
	UtilType       string    `column:"UTILITY_TYPE"`           //作业类型
	UtilOperType   string    `column:"UTILITY_OPERATION_TYPE"` //作业操作类型,dela,full,处于哪种状态等
	UtilInvokeType string    `column:"UTILITY_INVOKER_TYPE"`   //作业发起方式 AUTO自动，USER 人工
	UtilInvId      string    `column:"UTILITY_INVOCATION_ID"`  //作业唯一标识符
	ObjType        string    `column:"OBJECT_TYPE"`            //对象类型 表，索引
	ObjSchema      string    `column:"OBJECT_SCHEMA"`
	ObjName        string    `column:"OBJECT_NAME"`
	StmtText       string    `column:"REPLACE(UTILITY_DETAIL,CHR(10)||CHR(13),' ') AS UTILITY_DETAIL"` //具体的执行语句信息

}

func NewMonGetUtil() *MonGetUtil {
	m := new(MonGetUtil)
	m.rep = mon_get_rep
	m.tabname = "TABLE(MON_GET_UTILITY(-1))"
	m.start_flag = m.tabname + mon_get_start_flag
	m.end_flag = m.tabname + mon_get_end_flag
	return m
}

func (m *MonGetUtil) GetSqlText() string {
	return genSql(m)
}

//通过从数据库返回的结果，生成结果集
func GetMonGetUtilList(str string) []*MonGetUtil {
	m := NewMonGetUtil()
	ms := make([]*MonGetUtil, 0)
	start := strings.Index(str, m.start_flag) + len(m.start_flag)
	stop := strings.Index(str, m.end_flag)
	for _, line := range strings.Split(str[start:stop], "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := NewMonGetUtil()
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

func LookupMonGetUtilByUtilInvId(utils []*MonGetUtil, UtilInvId string) (*MonGetUtil, bool) {
	for _, u := range utils {
		if u.UtilInvId == UtilInvId {
			return u, true
		}
	}
	return nil, false
}
