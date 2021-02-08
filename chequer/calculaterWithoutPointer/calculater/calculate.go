package calculater

import "strconv"

func (postFix ValueNType) Calculate(stackMax int) string {
	var nowNum, postNum float64
	postFixTop := make(chan int, 1)
	postFixTop <- stackMax - 1
	result := make(ValueNType, stackMax)
	resultTop := make(chan int, 1)
	InitStack(resultTop)
	for _, v := range postFix {
		if v.V == ""{
			break
		}
		if v.T == "num" {
			result.Push(resultTop, stackMax, v.V)
		} else {
			postNum, _ = strconv.ParseFloat(result.Pop(resultTop).V, 64)
			nowNum, _ = strconv.ParseFloat(result.Pop(resultTop).V, 64)
			switch v.V {
			case "+":
				resultnum := strconv.FormatFloat(nowNum+postNum, 'g', -1, 64)
				result.Push(resultTop, stackMax, resultnum)
			case "-":
				resultnum := strconv.FormatFloat(nowNum-postNum, 'g', -1, 64)
				result.Push(resultTop, stackMax, resultnum)
			case "*":
				resultnum := strconv.FormatFloat(nowNum*postNum, 'g', -1, 64)
				result.Push(resultTop, stackMax, resultnum)
			case "/":
				resultnum := strconv.FormatFloat(nowNum/postNum, 'g', -1, 64)
				result.Push(resultTop, stackMax, resultnum)
			}
		}
	}
	return result.Pop(resultTop).V
}
