package protodb

import "strings"

func CensorWord(word string) string {
	if word == "" {
		return ""
	}
	wrunes := []rune(word)
	if len(wrunes) < 3 {
		return strings.Repeat("*", len(wrunes))
	}
	if len(wrunes) < 10 {
		return string(wrunes[0]) + strings.Repeat("*", len(wrunes)-2) + string(wrunes[len(wrunes)-1])
	}
	w1 := len(wrunes) / 10
	if w1 > 5 {
		w1 = 5
	}
	w2 := len(wrunes) / 15
	if w2 > 5 {
		w2 = 5
	}
	if w2 < 1 {
		w2 = 1
	}
	return string(wrunes[0:w1]) + strings.Repeat("*", len(wrunes)-w1-w2) + string(wrunes[len(wrunes)-w2:])
}
