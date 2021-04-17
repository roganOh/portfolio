package main

import (
	"calculater"
	"fmt"
)

func main() {
	var result string
	var postFixList calculater.ValueNType
	for continues := true; continues; calculater.CheckIfUserContinue(&continues) {
		inFixList := calculater.GetCorrectInFixList()
		fmt.Println(inFixList)
		postFixList = inFixList.InfixToPostfix()
		fmt.Println(postFixList)
		result = postFixList.Calculate()
		println("result: " + result)
	}

}

