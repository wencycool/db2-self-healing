package vm

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

//目前只支持redhat或者centos
//操作系统类型，操作系统版本，内核版本
type OsInfo struct {
	OsType      string
	OsLevel     string
	KernelLevel string
}

//获取挂载点信息
type MountPoint struct {
	FsName    string
	FsType    string
	TotalSize int
	UsedSize  int
	AvalSize  int
	Capacity  int
	MPoint    string
	User      *user.User
	Group     *user.Group
	MPInfo    *os.FileInfo
}

func (mp *MountPoint) Json() string {
	bs, _ := json.MarshalIndent(mp, "", " ")
	return string(bs)
}
func GetMountPoints() ([]*MountPoint, error) {
	mps := make([]*MountPoint, 0)
	var err error
	cmd := exec.Command("df", "-lTP")
	bs, err := cmd.CombinedOutput()
	if err != nil {
		return nil, errors.New(string(bs))
	}
	for _, line := range strings.Split(string(bs), "\n") {
		line_fields := strings.Fields(line)
		if len(line_fields) != 7 {
			continue
		}
		if line_fields[0] == "Filesystem" {
			continue
		}
		totalSize, _ := strconv.Atoi(line_fields[2])
		usedSize, _ := strconv.Atoi(line_fields[3])
		avalSize, _ := strconv.Atoi(line_fields[4])
		capacity, _ := strconv.Atoi(strings.Trim(line_fields[5], "%"))
		mpount := line_fields[6]
		finfo, err := os.Stat(mpount)
		if err != nil {
			return nil, err
		}
		muid := finfo.Sys().(*syscall.Stat_t).Uid
		mgid := finfo.Sys().(*syscall.Stat_t).Gid
		muser, err := user.LookupId(strconv.Itoa(int(muid)))
		groupname, err := user.LookupGroupId(strconv.Itoa(int(mgid)))
		mps = append(mps, &MountPoint{
			FsName:    line_fields[0],
			FsType:    line_fields[1],
			TotalSize: totalSize,
			UsedSize:  usedSize,
			AvalSize:  avalSize,
			Capacity:  capacity,
			MPoint:    mpount,
			User:      muser,
			Group:     groupname,
			MPInfo:    &finfo,
		})
	}
	return mps, err

}

//获取一个目录对应的文件系统使用情况
func GetMountPoint(str1 string) (*MountPoint, error) {
	mps, err := GetMountPoints()
	if err != nil {
		return nil, err
	}
	mps_MPoint_map := make(map[string]*MountPoint, 0)
	for _, mpoint := range mps {
		mps_MPoint_map[mpoint.MPoint] = mpoint
	}
	//如果循环超过1000次还没有找到则退出
	i := 0
	for {
		i++
		if _, ok := mps_MPoint_map[str1]; ok {
			return mps_MPoint_map[str1], nil
		} else if len(str1) == 0 || str1 == "." || i > 1000 {
			return nil, errors.New("Cannot find mount point")
		}
		str1 = filepath.Dir(str1)
	}

}

//创建用户,groupname中第一个是主组，其余为从组,返回已创建用户和错误信息
func CreateUser(username string, uid int, groupnames []string) (*user.User, error) {
	var (
		cmdStrParm string //创建用户名的命令
	)
	if len(groupnames) == 0 {
		return nil, errors.New("user must belongs to at lastest one group name")
	}
	//检查groupname是否不存在
	for _, gname := range groupnames {
		if _, err := user.LookupGroup(gname); err != nil {
			return nil, err
		}
	}
	//检查uid是否存在
	if _, err := user.LookupId(strconv.Itoa(uid)); err == nil {
		//说明用户ID已经存在
		return nil, errors.New("user id exists!")
	}
	//检查username是否存在
	if _user, err := user.Lookup(username); err == nil {
		//说明用户已经存在,没必要进行创建
		return _user, USER_EXISTS
	}
	//创建用户
	if len(groupnames) == 1 {
		cmdStrParm = fmt.Sprintf(" -u %d -g %s %s", uid, groupnames[0], username)
	} else {
		secondGroupNames := strings.Join(groupnames[1:], ",")
		cmdStrParm = fmt.Sprintf(" -u %d -g %s -G %s %s", uid, groupnames[0], secondGroupNames, username)
	}
	bs, err := exec.Command("/user/sbin/useradd", cmdStrParm).CombinedOutput()
	if err != nil {
		return nil, errors.New(string(bs))
	}
	return user.Lookup(username)
}

//创建用户组
func CreateGroup(groupname string, gid int) (*user.Group, error) {
	var (
		cmdPram string //创建用户组用到的参数
	)
	if _, err := user.LookupGroupId(strconv.Itoa(gid)); err == nil {
		return nil, errors.New(fmt.Sprintf("group id:%d alread exists!", gid))
	}
	if _group, err := user.LookupGroup(groupname); err == nil {
		return _group, GROUP_EXISTS
	}
	cmdPram = fmt.Sprintf(" -g %d %s", gid, groupname)
	bs, err := exec.Command("/usr/sbin/groupadd", cmdPram).CombinedOutput()
	if err != nil {
		return nil, errors.New(string(bs))
	}
	return user.LookupGroup(groupname)
}

//删除用户,返回已删除的用户信息和错误信息
func DeleteUser(username string) (*user.User, error) {
	var (
		cmdPram string //删除用户信息
		_user   *user.User
		err     error
	)
	//检查用户是否存在
	if _user, err = user.Lookup(username); err != nil {
		//用户不存在
		return nil, err
	}
	cmdPram = fmt.Sprintf(" %s", username)
	bs, err := exec.Command("/usr/sbin/userdel", cmdPram).CombinedOutput()
	if err != nil {
		//没有删除该用户
		return nil, errors.New(string(bs))
	}
	return _user, nil

}
