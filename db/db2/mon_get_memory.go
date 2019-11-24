package db2

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

/*
 Possible values for memory_set_type
DBMS Database manager memory set Instance
FMP Fenced mode process memory set Instance
PRIVATE Private memory set Instance
DATABASE Database memory set Database
APPLICATION Application memory set Database
FCM Fast communication manager (FCM) memory set Instance, Host
*/
type MemSetType string

const (
	MemSetType_DBMS        MemSetType = "DBMS"
	MemSetType_FMP         MemSetType = "FMP"
	MemSetType_PRIVATE     MemSetType = "PRIVATE"
	MemSetType_DATABASE    MemSetType = "DATABASE"
	MemSetType_APPLICATION MemSetType = "APPLICATION"
	MemSetType_FCM         MemSetType = "FCM"
)

/*
APM Agent pool management (APM) heap Internal memory pool
APPL_SHARED Application shared heap Internal memory pool
APPLICATION Application heap See applheapsz - Application heap size configuration parameter .
APS  APS heap  Internal memory pool
BSU_CF Base service utility (BSU) CF heap Internal memory pool
BSU Base service utility (BSU) heap Internal memory pool
BP Buffer pool heap See CREATE BUFFERPOOL statement .
CAT_CACHE Catalog cache heap See catalogcache_sz - Catalog cache size configuration parameter .
DATABASE_CF Database CF heap Internal memory pool
DATABASE Database heap See dbheap - Database heap configuration parameter .
DEBUG Debug heap Internal memory pool
DROP_INDEX Drop index heap Internal memory pool
EDU Engine dispatchable unit (EDU) heap Internal memory pool
FCMBP Fast communications manager (FCM) buffer heap See fcm_num_buffers - Number of FCM buffers configuration parameter .
FCM_CHANNEL FCM channel heap See fcm_num_channels - Number of FCM channels configuration parameter
FCM_CONTROL FCM control heap Internal memory pool
FCM_LOCAL FCM local heap Internal memory pool
FCM_SESSION FCM session heap Internal memory pool
FEDERATED Federated heap Internal memory pool
KERNEL_CONTROL Kernel control block heap Internal memory pool
KERNEL Kernel heap Internal memory pool
LOCK_MGR Lock manager heap See locklist - Maximum storage for lock list configuration parameter .
MISC Miscellaneous heap See DB2_FMP_COMM_HEAPSZ registry variable .
MONITOR Monitor heap See mon_heap_sz - Database system monitor heap size configuration parameter .
OPTPROF_PARSER OptProf XML parser heap Internal memory pool
OSS_TRACKER OSS resource tracking heap Internal memory pool
PERSISTENT_PRIVATE Persistent private heap Internal memory pool
PACKAGE_CACHE Package cache heap See pckcachesz - Package cache size configuration parameter .
PRIVATE Private Internal memory pool
RESYNC Resync heap Internal memory pool
SORT Private sort heap See sortheap - Sort heap size configuration parameter .
SHARED_SORT Shared sort heap See sheapthres_shr - Sort heap threshold for shared sorts configuration parameter .
SQL_COMPILER SQL compiler heap Internal memory pool
STATEMENT Statement heap See stmtheap - Statement heap size configuration parameter .
STATISTICS Statistics heap See stat_heap_sz - Statistics heap size configuration parameter .
USER_DATA User data heap Internal memory pool
UTILITY Utility heap See util_heap_sz - Utility heap size configuration parameter .
XMLCACHE XML cache heap Internal memory pool
XMLPARSER XML parser heap Internal memory pool

*/
//mempool种类太多，只列出常见几种
type MemPoolType string

const (
	MemPoolType_APPL_SHARED   MemPoolType = "APPL_SHARED"
	MemPoolType_APPLICATION   MemPoolType = "APPLICATION"
	MemPoolType_BP            MemPoolType = "BP"
	MemPoolType_DATABASE      MemPoolType = "DATABASE"
	MemPoolType_LOCK_MGR      MemPoolType = "LOCK_MGR"
	MemPoolType_MONITOR       MemPoolType = "MONITOR"
	MemPoolType_PACKAGE_CACHE MemPoolType = "PACKAGE_CACHE"
	MemPoolType_PRIVATE       MemPoolType = "PRIVATE"
	MemPoolType_SORT          MemPoolType = "SORT"
	MemPoolType_SHARED_SORT   MemPoolType = "SHARED_SORT"
	MemPoolType_STATEMENT     MemPoolType = "STATEMENT"
	MemPoolType_STATISTICS    MemPoolType = "STATISTICS"
	MemPoolType_UTILITY       MemPoolType = "UTILITY"
)

//获取当前数据库的memset基础数据
type MonGetMemSet struct {
	SnapTime        time.Time `column:"CURRENT TIMESTAMP"`
	HostName        string    `column:"HOST_NAME"`
	DbName          string    `column:"DB_NAME"`
	MemSetType      string    `column:"MEMORY_SET_TYPE"`
	MemSetId        int64     `column:"MEMORY_SET_ID"`
	MemSizeKb       int       `column:"MEMORY_SET_SIZE"`
	MemCmtSizeKb    int       `column:"MEMORY_SET_COMMITTED"`
	MemSetUsedKb    int       `column:"MEMORY_SET_USED"`
	MemSetUsedHWMKb int       `column:"MEMORY_SET_USED_HWM"`
}

func NewMonGetMemSetList() []*MonGetMemSet {
	m := new(MonGetMemSet)
	ms := make([]*MonGetMemSet, 0)
	sqlArg := fmt.Sprintf("select %s from TABLE(MON_GET_MEMORY_SET(NULL, CURRENT_SERVER, -1)) AS t with ur ", reflectMonGet(m))
	cmd := exec.Command("db2", "-x", "+p", sqlArg)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		log.Warn(err)
	}
	for _, line := range strings.Split(string(bs), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := new(MonGetMemSet)
		if err := renderStruct(d, line); err != nil {
			log.Warn(err)
			continue
		}
		ms = append(ms, d)
	}
	return ms
}

type MonGetMemSetList []*MonGetMemSet

func (m MonGetMemSetList) LookupMemSetBySetType(memsetype MemSetType) (*MonGetMemSet, error) {
	for _, v := range m {
		if v.MemSetType == string(memsetype) {
			return v, nil
		}
	}
	return nil, errors.New("没有找到该MemSetType")
}

//获取当前数据库的mempool基础数据,按照SET_TYPE、POOL_TYPE聚合
type MonGetMemPool struct {
	SnapTime      time.Time `column:"CURRENT TIMESTAMP"`
	HostName      string    `column:"HOST_NAME"`
	DbName        string    `column:"DB_NAME"`
	MemSetType    string    `column:"MEMORY_SET_TYPE"`
	MemPoolType   string    `column:"MEMORY_POOL_TYPE"`
	MemPoolUsedKb int       `column:"MEMORY_POOL_USED"`
	MemPoolHWMKb  int       `column:"MEMORY_POOL_USED_HWM"`
}

func NewMonGetMemPoolList() []*MonGetMemPool {
	m := new(MonGetMemPool)
	ms := make([]*MonGetMemPool, 0)
	sqlArg := fmt.Sprintf("select %s from (select HOST_NAME,DB_NAME,MEMORY_SET_TYPE,MEMORY_POOL_TYPE,sum(MEMORY_POOL_USED) as MEMORY_POOL_USED,sum(MEMORY_POOL_USED_HWM) as MEMORY_POOL_USED_HWM from TABLE(MON_GET_MEMORY_POOL(NULL, CURRENT_SERVER, -1)) AS t group by HOST_NAME,DB_NAME,MEMORY_SET_TYPE,MEMORY_POOL_TYPE) with ur ", reflectMonGet(m))
	cmd := exec.Command("db2", "-x", "+p", sqlArg)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		log.Warn(err)
	}
	for _, line := range strings.Split(string(bs), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := new(MonGetMemPool)
		if err := renderStruct(d, line); err != nil {
			log.Warn(err)
			continue
		}
		ms = append(ms, d)
	}
	return ms
}

type MonGetMemPoolList []*MonGetMemPool

//poolType在一个实例，一个数据库下经过聚合是唯一的
func (m MonGetMemPoolList) LookupMemPoolByPoolType(mempoolype MemPoolType) (*MonGetMemPool, error) {
	for _, v := range m {
		if v.MemPoolType == string(mempoolype) {
			return v, nil
		}
	}
	return nil, errors.New("没有找到该MemSetType")
}
