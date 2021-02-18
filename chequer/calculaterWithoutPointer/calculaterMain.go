package main

import (
	"calculater"
	"fmt"
)

func main() {
	var result string
	var postFixList calculater.ValueNType
	inFixList := calculater.GetCorrectInFixList()
	fmt.Println(inFixList)
	postFixList = inFixList.InfixToPostfix()
	fmt.Println(postFixList)
	//result = calculater.ValueNType.Calculate(postFixList) also can be like under code
	result = postFixList.Calculate()
	println(result)
}
