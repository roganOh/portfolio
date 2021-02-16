package calculater

type Element struct {
	V string
	T ElementType
}

type ValueNType []Element

type ElementType string

const (
	dot        ElementType = "dot"
	below      ElementType = "below"
	number     ElementType = "num"
	unknown    ElementType = "unknown"
	operater   ElementType = "operater"
	openbrace  ElementType = "openbrace"
	closebrace ElementType = "closebrace"
)

type OperaterType int

const (
	LBrace         OperaterType = 0
	MBrace         OperaterType = 1
	SBRace         OperaterType = 2
	plusMinus      OperaterType = 3
	multiplyDivide OperaterType = 4
	otherOperater  OperaterType = -1
)

type StateOfEqn int

const (
	OnlyMinus     StateOfEqn = -1
	Default       StateOfEqn = 0
	OnlyOneNumber StateOfEqn = 1
	Integer       StateOfEqn = 2
	NumberWithDot StateOfEqn = 3
	Decimal       StateOfEqn = 4
)

func ErrorWhenOtherComeAfterBelowMinus(eqn string, i int) {
	ErrorWithWhere(eqn, "only number can come after minus", i)
}
func ErrorWhenBelowMinusComeAfterDot(eqn string, i int) {
	ErrorWithWhere(eqn, "minus can't come right after number", i)
}
func ErrorWhenOperaterComeAfterDot(eqn string, i int) {
	ErrorWithWhere(eqn, "operater can't ome in decimal number", i)
}
func ErrorWhenBraceComeAfterDot(eqn string, i int) {
	ErrorWithWhere(eqn, "brace can't come right after number", i)
}
func ErrorWhenDotComAfterDot(eqn string, i int) {
	ErrorWithWhere(eqn, "dot can't come in decimal number", i)
}
func ErrorWhenBelowMinusComeAfterBelowMinus(eqn string, i int) {
	ErrorWithWhere(eqn, "minus can't come right after number", i)
}
