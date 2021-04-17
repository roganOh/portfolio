package calculater

import "fmt"

func ErrorWithWhere(eqn string, message ErrorMsg, i int) {
	var error string
	println(message)
	//println("       "+eqn)
	println(eqn)
	for a := 0; a < i; a++ {
		error += " "
	}
	println(error + "^")
	println("please write correct eqn")
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

			nowBrace := ValueNType.Pop(stack, braceTop).V
			matchBrace := func() string {
				switch ch{
				case ")":
					return "("
				case "}":
					return "{"
				case "]":
					return "["
				default:
					return ""
				}
			}()
			if nowBrace != matchBrace{
				return false
			}
		} else {
			continue
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

func isNotEmpty(eqn string) bool {
	for _, v := range eqn {
		ch := string(v)
		if IsDigit(ch) {
			return true
		}
	}
	return false
}

func isCorrectEqn(eqn string) bool {
	return isNotHaveTypo(eqn) && isHaveCorrectBrace(eqn) && isLastNumOrBrace(eqn) && isNotEmpty(eqn)
}

func GetCorrectEqn(eqn *string) {
	if *eqn == "" {
		fmt.Scan(eqn)
	}
	for !isCorrectEqn(*eqn) {
		println("wrong equation")
		fmt.Scan(eqn)
	}
}
