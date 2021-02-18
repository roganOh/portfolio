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
	postFixList = calculater.ValueNType.InfixToPostfix(inFixList)
	fmt.Println(postFixList)
	result = calculater.ValueNType.Calculate(postFixList)
	println(result)
}

