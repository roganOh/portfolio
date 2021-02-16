package calculater

import (
	"strconv"
)

func IsDigit(ch string) bool {
	_, err := strconv.ParseFloat(ch, 64)
	return err == nil
}

func IsTypeInTheseTypes(identified ElementType, typeGroup ...ElementType) bool {
	for _, v := range typeGroup {
		if identified == v {
			return true
		}
	}
	return false
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

func oneDigitEnd(this GroupParamsForGrouping) GroupParamsForGrouping {
	this.inFix[this.sequence] = this.num
	this.sequence++
	this.inFix[this.sequence] = this.ch
	this.sequence++
	this.state = Default
	this.num = ""
	return this
}

func defaultCase(eqn string, this GroupParamsForGrouping) GroupParamsForGrouping {
	elementType := TypeDefine(this.ch)
	switch elementType {
	case number:
		this.state = SingleDigit
		this.num += this.ch
	case below:
		this.state = OnlyMinus
		this.num += "-"
	case dot:
		ErrorDotComeAfterNone(eqn, this.i)
	case operater:
		this.inFix[this.sequence] = this.ch
		this.sequence++
	case openbrace, closebrace:
		//closebrace can't come in first because function isHaveCorrectBrace is working
		if this.sequence > 0 && IsTypeInTheseTypes(TypeDefine(this.inFix[this.sequence-1]), operater, openbrace) {
			this.inFix[this.sequence] = this.ch
			this.sequence++
		} else if this.sequence > 0 && IsTypeInTheseTypes(TypeDefine(this.inFix[this.sequence-1]), operater, openbrace) {
			ErrorBraceComeAfterNone(eqn, this.i)
		} else {
			this.inFix[this.sequence] = this.ch
			this.sequence++
		}

	}
	return this

}

func singleDigitCase(eqn string, this GroupParamsForGrouping) GroupParamsForGrouping {
	elementType := TypeDefine(this.ch)
	switch elementType {
	case number:
		this.state = Integer
		this.num += this.ch
	case below:
		ErrorBelowMinusComeAfterDigit(eqn, this.i)
	case operater, closebrace:
		this = oneDigitEnd(this)
	case openbrace:
		ErrorOpenBraceComeAfterSingleDigit(eqn, this.i)
	case dot:
		this.num += this.ch
		this.state = NumberWithDot
	}
	return this
}

func integerCase(eqn string, this GroupParamsForGrouping) GroupParamsForGrouping {
	elementType := TypeDefine(this.ch)
	switch elementType {
	case number:
		this.num += this.ch
	case below:
		ErrorBelowMinusComeAfterDigit(eqn, this.i)
	case operater, closebrace:
		this = oneDigitEnd(this)
	case openbrace:
		ErrorOpenBraceComeAfterInteger(eqn, this.i)
	case dot:
		this.num += this.ch
		this.state = NumberWithDot
	}
	return this
}

func decimalCase(eqn string, this GroupParamsForGrouping) GroupParamsForGrouping {
	elementType := TypeDefine(this.ch)
	switch elementType {
	case number:
		this.num += this.ch
	case below:
		ErrorBelowMinusComeAfterDigit(eqn, this.i)
	case operater, closebrace:
		this = oneDigitEnd(this)
	case openbrace:
		ErrorOpenBraceComeAfterBelowMinus(eqn, this.i)
	case dot:
		ErrorDotComeAfterBelowMinus(eqn, this.i)
	}
	return this
}

func groupingNumbers(eqn string) []string {
	this := GroupParamsForGrouping{inFix: make([]string, len(eqn))}
	var v int32
	for this.i, v = range eqn {
		this.ch = string(v)
		switch this.state {
		case OnlyMinus:
			if TypeDefine(this.ch) == number {
				this.state = SingleDigit
				this.num += this.ch
			} else {
				ErrorOtherComeAfterBelowMinus(eqn, this.i)
			}
		case Default:
			this = defaultCase(eqn, this)
		case SingleDigit:
			this = singleDigitCase(eqn, this)
		case Integer:
			this = integerCase(eqn, this)
		case NumberWithDot:
			if TypeDefine(this.ch) == number {
				this.num += this.ch
				this.state = Decimal
			} else {
				ErrorOtherComeAfterDot(eqn, this.i)
			}
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
