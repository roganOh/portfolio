package main

import (
	"calculater"
	"fmt"
)

func main() {
	var eqn string
	top := make(chan int, 1)
	fmt.Scan(&eqn)
	stackMax := 10
	for i, _ := range eqn {
		if i+2 >= stackMax {
			stackMax = stackMax << 1
		}else{continue}
	}
	inFixList := make(calculater.ValueNType, stackMax)
	for i, v := range eqn {
		ch := string(v)
		inFixList[i].V = ch
		inFixList[i].T = func() string {
			if calculater.IsDigit(ch) {
				return "num"
			} else {
				return "operation"
			}
		}()
	}
	calculater.InitStack(top)
	calculater.ValueNType.Push(inFixList,top,stackMax,"s")
	fmt.Println(calculater.ValueNType.Pop(inFixList, top))

	topValue,_ := <-top
	println(topValue)

}
