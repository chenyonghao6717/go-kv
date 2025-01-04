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
	strs_set := make(map[string]bool)
	for len(strs_set) < num {
		strs_set[RandStr(len_)] = true
	}
	strs := make([]string, 0, num)
	for key := range strs_set {
		strs = append(strs, key)
	}
	return strs
}
