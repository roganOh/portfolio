package main

import (
	"calculater"
	"fmt"
)

func main() {
	var eqn string
	var result string
	var inFixList calculater.ValueNType
	postFixList := make(calculater.ValueNType,len(eqn))

	calculater.GetCorrectEqn(&eqn)
	inFixList = calculater.MakeStringToStructStackWithType(eqn,inFixList)
	fmt.Println(inFixList)
	postFixList= calculater.ValueNType.InfixToPostfix(inFixList)
	fmt.Println(postFixList)
	result = calculater.ValueNType.Calculate(postFixList)
	println(result)
}

