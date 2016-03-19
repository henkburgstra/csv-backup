package main

import (
	"fmt"
	"strings"
	"testing"
)

func TestBackslashEscape(t *testing.T) {
	s := `test\\\\test`
	fmt.Println(s)
	fmt.Println(strings.Replace(s, `\`, `\\`, -1))
}
