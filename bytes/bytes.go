// Package bytes
package bytes

import (
	"unsafe"
)

func ToStr(b []bytes ) string {
	return (*[]byte)unsafe.Pointer(b)
}
