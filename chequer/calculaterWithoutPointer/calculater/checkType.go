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

func defaultCase(this GroupParamsForGrouping) (bool, GroupParamsForGrouping) {
	elementType := TypeDefine(this.ch)
	switch elementType {
	case number:
		this.state = Integer
		this.num += this.ch
	case below:
		this.state = OnlyMinus
		this.num += "-"
	case dot:
		this.msg = ErrorDotComeAfterNone
		return false, this
	case operater:
		this.inFix[this.sequence] = this.ch
		this.sequence++
	case openbrace, closebrace:
		//closebrace can't come in first because function isHaveCorrectBrace is working
		if this.sequence > 0 && IsTypeInTheseTypes(TypeDefine(this.inFix[this.sequence-1]), operater, openbrace) {
			this.inFix[this.sequence] = this.ch
			this.sequence++
		} else if this.sequence > 0 && IsTypeInTheseTypes(TypeDefine(this.inFix[this.sequence-1]), operater, openbrace) {
			this.msg = ErrorBraceComeAfterNone
			return false, this
		} else {
			this.inFix[this.sequence] = this.ch
			this.sequence++
		}

	}
	return true, this

}

func integerCase(this GroupParamsForGrouping) (bool, GroupParamsForGrouping) {
	elementType := TypeDefine(this.ch)
	switch elementType {
	case number:
		this.num += this.ch
	case below:
		this.msg = ErrorBelowMinusComeAfterDigit
		return false, this
	case operater, closebrace:
		this = oneDigitEnd(this)
	case openbrace:
		this.msg = ErrorOpenBraceComeAfterInteger
		return false, this
	case dot:
		this.num += this.ch
		this.state = NumberWithDot
	}
	return true, this
}

func decimalCase(this GroupParamsForGrouping) (bool, GroupParamsForGrouping) {
	elementType := TypeDefine(this.ch)
	switch elementType {
	case number:
		this.num += this.ch
	case below:
		this.msg = ErrorBelowMinusComeAfterDigit
		return false, this
	case operater, closebrace:
		this = oneDigitEnd(this)
	case openbrace:
		this.msg = ErrorOpenBraceComeAfterBelowMinus
		return false, this
	case dot:
		this.msg = ErrorDotComeAfterBelowMinus
		return false, this
	}
	return true, this
}

func groupingNumbers(eqn string) []string {
	this := GroupParamsForGrouping{inFix: make([]string, len(eqn))}
	var v int32
	var noErr bool = true
	for this.i, v = range eqn {
		this.ch = string(v)

		switch this.state {
		case OnlyMinus:
			if TypeDefine(this.ch) == number {
				this.state = Integer
				this.num += this.ch
			} else {
				this.msg = ErrorOtherComeAfterBelowMinus
				noErr = false
			}
		case Default:
			noErr, this = defaultCase(this)
		case Integer:
			noErr, this = integerCase(this)
		case NumberWithDot:
			if TypeDefine(this.ch) == number {
				this.num += this.ch
				this.state = Decimal
			} else {
				this.msg = ErrorOtherComeAfterDot
				noErr = true
			}
		case Decimal:
			noErr, this = decimalCase(this)
		}
		if !noErr {
			//if have error
			ErrorWithWhere(eqn, this.msg, this.i)
		}
		if this.num != "" {
			this.inFix[this.sequence] = this.num
		}
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
