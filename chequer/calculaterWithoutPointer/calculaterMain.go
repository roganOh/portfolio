package main

import (
	"calculater"
	"fmt"
)

func main() {
	var eqn string
	//top := make(chan int, 1)
	calculater.GetCorrectEqn(&eqn)
	stackMax := 10
	for i, _ := range eqn {
		if i+2 >= stackMax {
			stackMax = stackMax << 1
		}else{continue}
	}
	inFixList := make(calculater.ValueNType, stackMax)
	postFixList := make(calculater.ValueNType, stackMax)
	calculater.MakeStringToStructStackWithType(eqn,inFixList)
	fmt.Println(inFixList)
	postFixList = calculater.ValueNType.InfixToPostfix(inFixList,stackMax)
	fmt.Println(postFixList)

	//calculater.InitStack(top)
	//calculater.ValueNType.Push(inFixList,top,stackMax,"s")
	//fmt.Println(calculater.ValueNType.Pop(inFixList, top))



}
