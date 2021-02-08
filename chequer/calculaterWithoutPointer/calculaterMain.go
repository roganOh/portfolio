package main

import (
	"calculater"
	"fmt"
)

func main() {
	var eqn string
	var last int
	//top := make(chan int, 1)
	calculater.GetCorrectEqn(&eqn)
	//stackMax := 10
	//for i, _ := range eqn {
	//	if i+2 >= stackMax {
	//		stackMax = stackMax << 1
	//	}else{continue}
	//}
	inFixList := make(calculater.ValueNType, len(eqn))
	postFixList := make(calculater.ValueNType,len(eqn))
	calculater.MakeStringToStructStackWithType(eqn,inFixList)
	for i,v := range inFixList {
		if v.V == ""{
			last=i
			break
		}
		last =i+1
	}
	inFixList = inFixList[:last]
	fmt.Println(inFixList)
	postFixList = calculater.ValueNType.InfixToPostfix(inFixList,last)
	fmt.Println(postFixList)
	for i,v := range postFixList {
		if v.V == ""{
			last=i
			break
		}
		last =i+1
	}
	calculater.ValueNType.Calculate(postFixList,last)
}

