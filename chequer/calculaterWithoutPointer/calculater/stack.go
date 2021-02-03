package calculater

func Error(str string) {
	panic(str)
}

type Element struct {
	V string
	T string
}

type ValueNType []Element

func (data ValueNType) InitStack(top chan<- int) {
	top <- -1
}

func (data ValueNType) Pop(top chan int) Element {
	topValue, _ := <-top
	if topValue == -1 {
		Error("no more value in stack")
	}
	top <- topValue - 1
	return data[topValue-1]
}

func (data ValueNType) Push(top chan int, stackMax int, ch string) {
	topValue, _ := <-top
	if topValue >= stackMax {
		Error("stack overflow")
	}
	data[topValue+1].V = ch
	data[topValue+1].T = func() string {
		if IsDigit(ch) {
			return "num"
		} else {
			return "operation"
		}
	}()
	top <- topValue + 1
}

func (data ValueNType) Peek(top chan int) Element {
	topValue, _ := <-top
	if topValue == -1 {
		Error("no more value in stack")
	}
	top <- topValue
	return data[topValue]
}
