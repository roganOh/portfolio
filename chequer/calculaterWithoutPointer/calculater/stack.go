package calculater

func Error(str string) {
	panic(str)
}

func InitStack(top ...chan<- int) {
	for _,c := range top {
		c <- -1
	}
}

func (data ValueNType) IsStackEmpty(top chan int) bool {
	topValue, _ := <- top
	top <- topValue
	if topValue == -1 {
		return true
	} else {
		return false
	}
}

func (data ValueNType) Pop(top chan int) Element {
	topValue, _ := <-top
	if topValue == -1 {
		Error("no more value in stack")
	}
	top <- topValue - 1
	return data[topValue]
}

func (data ValueNType) Push(top chan int, stackMax int, ch string) {
	topValue, _ := <-top
	if topValue >= stackMax {
		Error("stack overflow")
	}
	data[topValue+1].V = ch
	data[topValue+1].T = TypeDefine(ch)
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
