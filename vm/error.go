package vm

import "errors"

var USER_EXISTS error = errors.New("user exists")
var GROUP_EXISTS error = errors.New("group exists")
