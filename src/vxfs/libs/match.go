package libs

import (
	"regexp"
)

func IsIntegerText(text string) bool {
	m, _ := regexp.MatchString("^[1-9]{1}[0-9]*$", text)
	return m
}
