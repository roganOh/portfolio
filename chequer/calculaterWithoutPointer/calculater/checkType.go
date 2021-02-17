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
	this.inFix = append(this.inFix, Element{this.num, TypeDefine(this.num)})
	this.inFix = append(this.inFix, Element{this.ch, TypeDefine(this.ch)})
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
		this.inFix = append(this.inFix, Element{this.ch, TypeDefine(this.ch)})
	case openbrace:
		if len(this.inFix) ==0 {
			this.inFix = append(this.inFix, Element{this.ch, TypeDefine(this.ch)})
		} else if IsTypeInTheseTypes(this.inFix[len(this.inFix)-1].T, operater, openbrace) {
			this.inFix = append(this.inFix, Element{this.ch, TypeDefine(this.ch)})
		} else {
			this.msg = ErrorBraceLocateIsWrong
			return false, this
		}
	case closebrace:
		//closebrace can't come in first because function isHaveCorrectBrace is working
		if IsTypeInTheseTypes(this.inFix[len(this.inFix)-1].T, closebrace, openbrace) {
			this.inFix = append(this.inFix, Element{this.ch, TypeDefine(this.ch)})
		} else {
			this.msg = ErrorBraceLocateIsWrong
			return false, this
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

func MakeInFixWithMappingType(eqn string, inFixList ValueNType) (bool,ValueNType) {
	this := GroupParamsForGrouping{inFix: inFixList}
	var v int32
	var noErr bool = true
	for this.i, v = range eqn {
		this.ch = string(v)
		switch this.state {
		case Default:
			noErr, this = defaultCase(this)
		case OnlyMinus:
			if TypeDefine(this.ch) == number {
				this.state = Integer
				this.num += this.ch
			} else {
				this.msg = ErrorOtherComeAfterBelowMinus
				noErr = false
			}
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
			return false,this.inFix
		}

	}
	if this.num != "" {
		this.inFix = append(this.inFix, Element{this.num, TypeDefine(this.num)})
	}

	return true, this.inFix
}
