package calculater

func checkPriority(ch string) int {
	switch ch {
	case "[", "]":
		return 0
	case "{", "}":
		return 1
	case "(", ")":
		return 2
	case "+", "-":
		return 3
	case "*", "/":
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
	for i := 0; i < stackMax; i++ {
		if inFix[i].T == "num" {
			ValueNType.Push(postFix, postFixTop, stackMax, inFix[i].V)
		} else if inFix[i].T == "openbrace" {
			ValueNType.Push(operators, operatorsTop, stackMax, inFix[i].V)
		} else if inFix[i].T == "closebrace" {
			switch inFix[i].V {
			case "]":
				for {
					stackResult := ValueNType.Pop(operators, operatorsTop).V
					if stackResult == "[" {
						break
					}
					ValueNType.Push(postFix, postFixTop, stackMax, stackResult)
				}
			case "}":
				for {
					stackResult := ValueNType.Pop(operators, operatorsTop).V
					if stackResult == "{" {
						break
					}
					ValueNType.Push(postFix, postFixTop, stackMax, stackResult)
				}
			case ")":
				for {
					stackResult := ValueNType.Pop(operators, operatorsTop).V
					if stackResult == "(" {
						break
					}
					ValueNType.Push(postFix, postFixTop, stackMax, stackResult)
				}
			}
		} else {
			for !operators.IsStackEmpty(operatorsTop) {
				op=checkPriority(operators.Peek(operatorsTop).V)
				if  op >= checkPriority(inFix[i].V) {
					postFix.Push(postFixTop, stackMax, ValueNType.Pop(operators, operatorsTop).V)
				} else {
					break
				}
			}
			ValueNType.Push(operators, operatorsTop, stackMax, inFix[i].V)
		}
	}
	for !operators.IsStackEmpty(operatorsTop) {
		if ValueNType.Peek(operators, operatorsTop).V == "" {
			break
		}
		ValueNType.Push(postFix, postFixTop, stackMax, operators.Pop(operatorsTop).V)
	}
	close(operatorsTop)
	return postFix
}
