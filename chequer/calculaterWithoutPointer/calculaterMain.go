package main

import (
	"calculater"
	"fmt"
)

func main() {
	var eqn string
	var result string
	var inFixList calculater.ValueNType
	var postFixList calculater.ValueNType
	calculater.GetCorrectEqn(&eqn)
	inFixList = calculater.MakeInFixWithMappingType(eqn, inFixList)
	fmt.Println(inFixList)
	postFixList = calculater.ValueNType.InfixToPostfix(inFixList)
	fmt.Println(postFixList)
	result = calculater.ValueNType.Calculate(postFixList)
	println(result)
}

