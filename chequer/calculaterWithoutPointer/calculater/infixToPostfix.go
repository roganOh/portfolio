package calculater

func checkPriority(ch string) OperaterType {
	switch ch {
	case "[", "]":
		return LBrace
	case "{", "}":
		return MBrace
	case "(", ")":
		return SBRace
	case "+", "-":
		return plusMinus
	case "*", "/":
		return multiplyDivide
	}
	return otherOperater
}

func popFromOperators(this GroupParamsForPostfixer) string {
	return this.operators.Pop(this.operatorsTop).V
}

func popNPushUntilBraceMatch(nowElement string, this GroupParamsForPostfixer, stackMax int) {
	var matchBrace string
	switch nowElement {
	case "]":
		matchBrace = "["
	case "}":
		matchBrace = "{"
	case ")":
		matchBrace = "("
	}
	for stackResult := popFromOperators(this); stackResult != matchBrace; stackResult = popFromOperators(this) {
		//init stackResult and while stackResult != matchBrace, run for func and also do assign value in stackResult
		ValueNType.Push(this.postFix, this.postFixTop, stackMax, stackResult)
	}
}

func pushOperators(e Element,this GroupParamsForPostfixer,stackMax int) GroupParamsForPostfixer {
	for !this.operators.IsStackEmpty(this.operatorsTop) {
		this.op = checkPriority(this.operators.Peek(this.operatorsTop).V)
		if this.op >= checkPriority(e.V) {
			this.postFix.Push(this.postFixTop, stackMax, ValueNType.Pop(this.operators, this.operatorsTop).V)
		} else {
			break
		}
	}

	ValueNType.Push(this.operators, this.operatorsTop, stackMax, e.V)
	return this
}

func (inFix ValueNType) InfixToPostfix() ValueNType {
	stackMax := len(inFix)
	this := GroupParamsForPostfixer{
		operators:    make(ValueNType, stackMax),
		operatorsTop: make(chan int, 1),
		postFix:      make(ValueNType, stackMax),
		postFixTop:   make(chan int, 1),
	}
	InitStack(this.operatorsTop,this.postFixTop)
	for i := 0; i < stackMax; i++ {

		if inFix[i].T == number {
			ValueNType.Push(this.postFix, this.postFixTop, stackMax, inFix[i].V)
		} else if inFix[i].T == openbrace {
			ValueNType.Push(this.operators, this.operatorsTop, stackMax, inFix[i].V)
		} else if inFix[i].T == closebrace {
			popNPushUntilBraceMatch(inFix[i].V, this, stackMax)
		} else {
			this = pushOperators(inFix[i],this,stackMax)
		}

	}
	for !this.operators.IsStackEmpty(this.operatorsTop) {
		if ValueNType.Peek(this.operators, this.operatorsTop).V == "" {
			break
		}
		ValueNType.Push(this.postFix, this.postFixTop, stackMax, this.operators.Pop(this.operatorsTop).V)
	}
	close(this.operatorsTop)
	return this.postFix
}
