package calculater

type Element struct {
	V string
	T ElementType
}

type ValueNType []Element
type StateOfEqn int
type ElementType string
type OperaterType int

type GroupParamsForGrouping struct {
	i        int
	ch       string
	state    StateOfEqn
	inFix    []string
	num      string
	sequence int
}

type GroupParamsForPostfixer struct {
	operators ValueNType
	operatorsTop chan int
	postFix ValueNType
	postFixTop chan int
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
	SingleDigit   StateOfEqn = 1
	Integer       StateOfEqn = 2
	NumberWithDot StateOfEqn = 3
	Decimal       StateOfEqn = 4
)

func ErrorOtherComeAfterBelowMinus(eqn string, i int) {
	ErrorWithWhere(eqn, "only number can come after minus", i)
}
func ErrorOtherComeAfterDot(eqn string, i int) {
	ErrorWithWhere(eqn, "only number can come right after dot", i)
}
func ErrorOpenBraceComeAfterBelowMinus(eqn string, i int) {
	ErrorWithWhere(eqn, "open brace can't come right after number", i)
}
func ErrorDotComeAfterBelowMinus(eqn string, i int) {
	ErrorWithWhere(eqn, "dot can't come in decimal number", i)
}
func ErrorOpenBraceComeAfterInteger(eqn string, i int) {
	ErrorWithWhere(eqn, "open brace can't come right after number", i)
}
func ErrorOpenBraceComeAfterSingleDigit(eqn string, i int) {
	ErrorWithWhere(eqn, "open brace can't come right after number", i)
}
func ErrorBelowMinusComeAfterDigit(eqn string, i int) {
	ErrorWithWhere(eqn, "minus can't come right after number", i)
}
func ErrorBraceComeAfterNone(eqn string, i int) {
	ErrorWithWhere(eqn, "brace can't come after None", i)
}
func ErrorDotComeAfterNone(eqn string, i int) {
	ErrorWithWhere(eqn, "dot can't come after number or operater", i)
}

