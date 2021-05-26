// Package hash
package hash

// FnvHash 实现原来 java 版本的 fnv 算法，和 golang 的标准库计算方式不完全一致
func FnvHash(key []byte) uint32 {
	const (
		p = 16777619
	)
	// 此处为了兼容 java 的错误初始值，正确的值应该是 2166136261
	var hash int64 = -2128831035
	for _, b := range key {
		hash = (hash ^ int64(b)) * p
	}
	hash += (hash << 13)
	hash ^= (hash >> 7)
	hash += (hash << 3)
	hash ^= (hash >> 17)
	hash += (hash << 5)

	return uint32(hash & 0x7FFFFFFF)
}
