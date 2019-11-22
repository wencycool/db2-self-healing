package db2

import (
	"fmt"
	"os/exec"
	"strings"
	"time"
)

//该部分为表空间自动扩容提供基础数据支撑
//表空间相关信息
type MonGetTbsp struct {
	SnapTime      time.Time `column:"CURRENT TIMESTAMP"`
	TbspName      string    `column:"TBSP_NAME"`
	TbspId        int64     `column:"TBSP_ID"`   //已执行时间毫秒
	TbspType      string    `column:"TBSP_TYPE"` //DMS SMS
	State         string    `column:"TBSP_STATE"`
	ContType      string    `column:"TBSP_CONTENT_TYPE"` //ANY LARGE SYSTEMP USRTEMP
	PageSize      int64     `column:"TBSP_PAGE_SIZE"`    //byte
	ExtendSize    int64     `column:"TBSP_EXTENT_SIZE"`
	FsCaching     int64     `column:"FS_CACHING"`                // If fs_caching is 0, file system caching is enabled. If fs_caching is 1, file system caching is disabled
	AutoStorage   int64     `column:"TBSP_USING_AUTO_STORAGE"`   //A value of 1 means "Yes"; a value of 0 means "No"
	AutoResize    int64     `column:"TBSP_AUTO_RESIZE_ENABLED"`  //A value of 1 means "Yes"; a value of 0 means "No"
	ReclaimEnable int64     `column:"RECLAIMABLE_SPACE_ENABLED"` //If the table space is enabled for reclaimable storage, then this monitor element returns a value of 1. Otherwise, it returns a value of 0
	UsedPages     int       `column:"TBSP_USED_PAGES"`
	FreePages     int       `column:"TBSP_FREE_PAGES"`
	UsablePages   int       `column:"TBSP_USABLE_PAGES"`
	TotalPages    int       `column:"TBSP_TOTAL_PAGES"`
	PendingPages  int       `column:"TBSP_PENDING_FREE_PAGES"`
	HWM           int       `column:"TBSP_PAGE_TOP"`
	MaxHWM        int       `column:"TBSP_MAX_PAGE_TOP"`  //The highest allocated page number for a DMS table space since the database was activated.
	StorageName   string    `column:"STORAGE_GROUP_NAME"` //Name of a storage group.
	StorageId     int64     `column:"STORAGE_GROUP_ID"`   //An integer that uniquely represents a storage group used by the current database.
}

//获取表空间信息列表
func NewMonGetTbspList() []*MonGetTbsp {
	m := new(MonGetTbsp)
	ms := make([]*MonGetTbsp, 0)
	sqlArg := fmt.Sprintf("select %s from TABLE(MON_GET_TABLESPACE('',-1)) as t with ur ", reflectMonGet(m))
	cmd := exec.Command("db2", "-x", "+p", sqlArg)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		log.Warn(string(bs))
	}
	for _, line := range strings.Split(string(bs), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := new(MonGetTbsp)
		if err := renderStruct(d, line); err != nil {
			log.Warn(err)
			continue
		}
		ms = append(ms, d)
	}

	return ms
}

type MonGetContainer struct {
	SnapTime      time.Time `column:"CURRENT TIMESTAMP"`
	TbspName      string    `column:"TBSP_NAME"`
	TbspId        int64     `column:"TBSP_ID"` //已执行时间毫秒
	ContainName   string    `column:"CONTAINER_NAME"`
	ContainId     int64     `column:"CONTAINER_ID"`
	ContainType   string    `column:"CONTAINER_TYPE"` //DISK_EXTENT_TAG DISK_PAGE_TAG FILE_EXTENT_TAG FILE_EXTENT_TAG PATH
	StripeSet     int64     `column:"STRIPE_SET"`
	TotalPages    int       `column:"TOTAL_PAGES"`
	UsablePages   int       `column:"USABLE_PAGES"`
	Accessiable   int64     `column:"ACCESSIBLE"`
	FsId          string    `column:"FS_ID"` //文件系统挂载点或者设备的Dev，finfo,_ := os.Stat("/"),reflect.ValueOf(finfo.Sys()).Elem().FieldByName("Dev").Uint()
	FsTotalSize   int       `column:"FS_TOTAL_SIZE"`
	FsUsedSize    int       `column:"FS_USED_SIZE"`
	StoragePathId int64     `column:"DB_STORAGE_PATH_ID"`
}

//获取表空间容器信息列表
func NewMonGetContainerList() []*MonGetContainer {
	m := new(MonGetContainer)
	ms := make([]*MonGetContainer, 0)
	sqlArg := fmt.Sprintf("select %s from TABLE(MON_GET_CONTAINER('',-1)) AS t with ur ", reflectMonGet(m))
	cmd := exec.Command("db2", "-x", "+p", sqlArg)
	bs, err := cmd.CombinedOutput()
	if err != nil {
		log.Warn(err)
	}
	for _, line := range strings.Split(string(bs), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		d := new(MonGetContainer)
		if err := renderStruct(d, line); err != nil {
			log.Warn(err)
			continue
		}
		ms = append(ms, d)
	}
	return ms
}

/*
In administrative views and table functions, this monitor element returns a text identifier based on defines in sqlutil.h, and is combination of the following values separated by a '+' sign:
BACKUP_IN_PROGRESS
BACKUP_PENDING
DELETE_PENDING
DISABLE_PENDING
DROP_PENDING
LOAD_IN_PROGRESS
LOAD_PENDING
MOVE_IN_PROGRESS
NORMAL
OFFLINE
PSTAT_CREATION
PSTAT_DELETION
QUIESCED_EXCLUSIVE
QUIESCED_SHARE
QUIESCED_UPDATE
REBAL_IN_PROGRESS
REDIST_IN_PROGRESS
REORG_IN_PROGRESS
RESTORE_IN_PROGRESS
RESTORE_PENDING
ROLLFORWARD_IN_PROGRESS
ROLLFORWARD_PENDING
STORDEF_ALLOWED
STORDEF_CHANGED
STORDEF_FINAL_VERSION
STORDEF_PENDING
SUSPEND_WRITE
This element contains a hexadecimal value indicating the current table space state. The externally visible state of a table space is composed of the hexadecimal sum of certain state values. For example, if the state is "quiesced: EXCLUSIVE" and "Load pending", the value is 0x0004 + 0x0008, which is 0x000c. Use the db2tbst command to obtain the table space state associated with a given hexadecimal value.
Table 3. Bit definitions listed in sqlutil.h Hexadecimal Value Decimal Value State
0x0 0 Normal (see the definition SQLB_NORMAL in sqlutil.h)
0x1 1 Quiesced: SHARE
0x2 2 Quiesced: UPDATE
0x4  4 Quiesced: EXCLUSIVE
0x8 8 Load pending
0x10  16 Delete pending
0x20  32 Backup pending
0x40  64 Roll forward in progress
0x80  128 Roll forward pending
0x100  256 Restore pending
0x100  256 Recovery pending (not used)
0x200  512 Disable pending
0x400  1024 Reorg in progress
0x800  2048 Backup in progress
0x1000  4096 Storage must be defined
0x2000  8192 Restore in progress
0x4000  16384 Offline and not accessible
0x8000 32768 Drop pending
0x10000 65536 No write is allowed
0x20000 131072 Load in progress
0x40000 262144 Redistribute in progress
0x80000 524288 Move in progress
0x2000000  33554432 Storage may be defined
0x4000000  67108864 Storage Definition is in 'final' state
0x8000000  134217728 Storage Definition was changed before rollforward
0x10000000  268435456 DMS rebalancer is active
0x20000000  536870912 TBS deletion in progress
0x40000000  1073741824 TBS creation in progress

*/
