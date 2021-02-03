package calculater

import "strconv"

func IsDigit(ch string) bool {
	_,err := strconv.Atoi(ch)
	return err == nil
}

