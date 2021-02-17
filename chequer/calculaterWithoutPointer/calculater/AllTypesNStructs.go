package calculater

type Element struct {
	V string
	T ElementType
}

type ValueNType []Element
type StateOfEqn int
type ElementType string
type OperaterType int
type ErrorMsg string

type GroupParamsForGrouping struct {
	i     int
	ch    string
	state StateOfEqn
	inFix ValueNType
	num   string
	msg   ErrorMsg
}

type GroupParamsForPostfixer struct {
	operators    ValueNType
	operatorsTop chan int
	postFix      ValueNType
	postFixTop   chan int
}

const (
	dot        ElementType = "dot"
	below      ElementType = "below"
	number     ElementType = "num"
	unknown    ElementType = "unknown"
	operater   ElementType = "operater"
	openbrace  ElementType = "openbrace"
	closebrace ElementType = "closebrace"
)

const (
	LBrace         OperaterType = 0
	MBrace         OperaterType = 1
	SBRace         OperaterType = 2
	plusMinus      OperaterType = 3
	multiplyDivide OperaterType = 4
	otherOperater  OperaterType = -1
)

const (
	OnlyMinus     StateOfEqn = -1
	Default       StateOfEqn = 0
	Integer       StateOfEqn = 1
	NumberWithDot StateOfEqn = 2
	Decimal       StateOfEqn = 3
)

const (
	ErrorOtherComeAfterBelowMinus     ErrorMsg = "only number can come after minus"
	ErrorOtherComeAfterDot            ErrorMsg = "only number can come right after dot"
	ErrorOpenBraceComeAfterBelowMinus ErrorMsg = "open brace can't come right after number"
	ErrorDotComeAfterBelowMinus       ErrorMsg = "dot can't come in decimal number"
	ErrorOpenBraceComeAfterInteger    ErrorMsg = "open brace can't come right after number"
	ErrorBelowMinusComeAfterDigit     ErrorMsg = "minus can't come right after number"
	ErrorBraceLocateIsWrong           ErrorMsg = "brace locate error"
	ErrorDotComeAfterNone             ErrorMsg = "dot can't come after number or operater"
)
