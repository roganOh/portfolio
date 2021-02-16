package calculater

import (
	"strconv"
)

func IsDigit(ch string) bool {
	_, err := strconv.ParseFloat(ch, 64)
	return err == nil
}

func IsNotDigit(ch string) bool {
	return IsDigit(ch) == false
}

func TypeDefine(ch string) ElementType {
	if IsDigit(ch) {
		return number
	} else {
		switch ch {
		case "_":
			return below
		case "+", "-", "*", "/":
			return operater
		case "[", "{", "(":
			return openbrace
		case "]", "}", ")":
			return closebrace
		case ".":
			return dot
		}
	}
	return unknown
}

func defaultCase(eqn string, this groupForLessSwitch) groupForLessSwitch {
	elementType := TypeDefine(this.ch)
	switch elementType {
	case number:
		this.state = OnlyOneNumber
		this.num += this.ch
	case below:
		this.state = OnlyMinus
		this.num += "-"
	case dot:
		ErrorWithWhere(eqn, "dot can't come after number or operater", this.i)
	case closebrace:
		if TypeDefine(this.inFix[this.sequence-1]) == closebrace {
			this.inFix[this.sequence] = this.ch
			this.sequence++
		} else {
			ErrorWithWhere(eqn, "close brace can't come after operater", this.i)
		}
	case operater:
		this.inFix[this.sequence] = this.ch
		this.sequence++
	case openbrace:
		if this.sequence > 0 {
			if TypeDefine(this.inFix[this.sequence-1]) == operater {
				this.inFix[this.sequence] = this.ch
				this.sequence++
			} else {
				ErrorWithWhere(eqn, "open brace error", this.i)
			}
		} else {
			this.inFix[this.sequence] = this.ch
			this.sequence++
		}

	}
	return this

}

func onlyOneNumberCase(eqn string, this groupForLessSwitch) groupForLessSwitch {
	elementType := TypeDefine(this.ch)
	switch elementType {
	case number:
		this.state = Integer
		this.num += this.ch
	case below:
		ErrorWithWhere(eqn, "minus can't come right after number", this.i)
	case operater, closebrace:
		this.inFix[this.sequence] = this.num
		this.sequence++
		this.inFix[this.sequence] = this.ch
		this.sequence++
		this.state = Default
		this.num = ""
	case openbrace:
		ErrorWithWhere(eqn, "open brace can't come right after number", this.i)
	case dot:
		this.num += this.ch
		this.state = NumberWithDot
	}
	return this
}

func integerCase(eqn string, this groupForLessSwitch) groupForLessSwitch {
	elementType := TypeDefine(this.ch)
	switch elementType {
	case number:
		this.num += this.ch
	case below:
		ErrorWithWhere(eqn, "minus can't come right after number", this.i)
	case operater, closebrace:
		this.inFix[this.sequence] = this.num
		this.sequence++
		this.inFix[this.sequence] = this.ch
		this.sequence++
		this.state = Default
		this.num = ""
	case openbrace:
		ErrorWithWhere(eqn, "open brace can't come right after number", this.i)
	case dot:
		this.num += this.ch
		this.state = NumberWithDot
	}
	return this
}

func decimalCase(eqn string, this groupForLessSwitch) groupForLessSwitch {
	elementType := TypeDefine(this.ch)
	switch elementType {
	case number:
		this.num += this.ch
	case below:
		ErrorWhenBelowMinusComeAfterBelowMinus(eqn, this.i)
	case operater, closebrace:
		this.inFix[this.sequence] = this.num
		this.sequence++
		this.inFix[this.sequence] = this.ch
		this.sequence++
		this.state = Default
		this.num = ""
	case openbrace:
		ErrorWithWhere(eqn, "open brace can't come right after number", this.i)
	case dot:
		ErrorWithWhere(eqn, "dot can't come in decimal number", this.i)
	}
	return this
}

func numberWithDotCase(eqn string, this groupForLessSwitch) groupForLessSwitch {
	elementType := TypeDefine(this.ch)
	switch elementType {
	case number:
		this.num += this.ch
		this.state = Decimal
	case below:
		ErrorWhenBelowMinusComeAfterDot(eqn, this.i)
	case operater:
		ErrorWhenOperaterComeAfterDot(eqn, this.i)
	case openbrace, closebrace:
		ErrorWhenBraceComeAfterDot(eqn, this.i)
	case dot:
		ErrorWhenDotComAfterDot(eqn, this.i)
	}
	return this
}

type groupForLessSwitch struct {
	i        int
	ch       string
	state    StateOfEqn
	inFix    []string
	num      string
	sequence int
}

func groupingNumbers(eqn string) []string {
	this := groupForLessSwitch{inFix: make([]string, len(eqn))}
	var v int32
	for this.i, v = range eqn {
		this.ch = string(v)
		switch this.state {
		case OnlyMinus:
			if TypeDefine(this.ch) == number {
				this.state = OnlyOneNumber
				this.num += this.ch
			} else {
				ErrorWhenOtherComeAfterBelowMinus(eqn, this.i)
			}
		case Default:
			this = defaultCase(eqn, this)
		case OnlyOneNumber:
			this = onlyOneNumberCase(eqn, this)
		case Integer:
			this = integerCase(eqn, this)
		case NumberWithDot:
			this = numberWithDotCase(eqn, this)
		case Decimal:
			this = decimalCase(eqn, this)
		}
	}
	if this.num != "" {
		this.inFix[this.sequence] = this.num
	}
	return this.inFix
}

func MakeStringToStructStackWithType(eqn string, inFixList ValueNType) ValueNType {
	equationList := groupingNumbers(eqn)
	for _, v := range equationList {
		inFixList = append(inFixList, Element{v, TypeDefine(v)})
	}
	return inFixList
}
