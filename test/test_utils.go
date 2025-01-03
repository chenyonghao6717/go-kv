package test

import (
	"math/rand"
	"strings"
)

const (
	charSet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

func RandStr(len_ int) string {
	var sb strings.Builder
	for i := 0; i < len_; i++ {
		idx := rand.Intn(len(charSet))
		sb.WriteByte(charSet[idx])
	}
	return sb.String()
}

func RandStrs(len_ int, num int) []string {
	strs := make([]string, num)
	for i := 0; i < num; i++ {
		strs[i] = RandStr(len_)
	}
	return strs
}
