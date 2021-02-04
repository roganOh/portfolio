package calculater

func checkPriority(ch string) int {
	switch ch {
	case "[", "]":
		return 0
	case "{", "}":
		return 1
	case "(", ")":
		return 2
	case "*", "/":
		return 3
	case "+", "-":
		return 4
	}
	return -1
}

func (inFix ValueNType) InfixToPostfix(stackMax int) ValueNType {
	var op int
	postFix := make(ValueNType, stackMax)
	postFixTop := make(chan int, 1)
	operators := make(ValueNType, stackMax)
	operatorsTop := make(chan int, 1)
	InitStack(operatorsTop)
	InitStack(postFixTop)
	for i := 0; i <= stackMax; i++ {
		if inFix[i].T == "num" {
			ValueNType.Push(postFix, postFixTop, stackMax, inFix[i].V)
		} else {
			for op <= checkPriority(inFix[i].V) {
				ValueNType.Push(postFix,postFixTop,stackMax,ValueNType.Pop(operators,operatorsTop).V)
			}
			ValueNType.Push(operators,operatorsTop,stackMax,inFix[i].V)
			op = checkPriority(inFix[i].V)
		}
	}
	return postFix
}
