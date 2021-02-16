package calculater

import "fmt"

func ErrorWithWhere(eqn string,message string,i int) {
	var error string
	println(message)
	println("       "+eqn)
	for a := 0; a < i; a++ {
		error += " "
	}
	Error(error + "^")
}
func isNotHaveTypo(eqn string) bool {
	for _, v := range eqn {
		ch := string(v)
		if TypeDefine(ch) == unknown {
			return false
		}
	}
	return true
}

func isHaveCorrectBrace(eqn string) bool {
	var topValue int
	braceTop := make(chan int, 1)
	stackMax := len(eqn)
	stack := make(ValueNType, stackMax)
	InitStack(braceTop)

	for _, v := range eqn {
		ch := string(v)
		if TypeDefine(ch) == openbrace {
			ValueNType.Push(stack, braceTop, stackMax, ch)
		} else if TypeDefine(ch) == closebrace {
			if topValue = <-braceTop; topValue == -1 {
				return false
			}
			braceTop <- topValue
			matchBrace := ValueNType.Pop(stack, braceTop).V
			switch ch {
			case ")":
				if matchBrace != "(" {
					return false
				}
			case "}":
				if matchBrace != "{" {
					return false
				}
			case "]":
				if matchBrace != "[" {
					return false
				}
			}
		}
	}

	if topValue = <-braceTop; topValue != -1 {
		return false
	}
	return true
}

func isLastNumOrBrace(eqn string) bool {
	lastChar := string(eqn[len(eqn)-1])
	return TypeDefine(lastChar) == number || TypeDefine(lastChar) == closebrace
}

func isCorrectEqn(eqn string) bool {
	return isNotHaveTypo(eqn) && isHaveCorrectBrace(eqn) && isLastNumOrBrace(eqn)
}

func GetCorrectEqn(eqn *string) {
	if *eqn == "" {
		fmt.Scan(eqn)
	}
	for !isCorrectEqn(*eqn){
		println("wrong equation please write again")
		fmt.Scan(eqn)
	}
}