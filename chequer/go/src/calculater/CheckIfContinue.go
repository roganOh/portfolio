package calculater

import (
	"fmt"
	"strings"
)

func CheckIfUserContinue(continues *bool) {
	println("conintue? [y/n]")
	var UserInput string
	fmt.Scan(&UserInput)
	if strings.ToLower(UserInput) == "y" {
		*continues = true
	} else {
		*continues = false
	}

}
