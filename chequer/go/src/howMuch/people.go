package howMuch

type People struct {
	Name string
	Money float64
	PayDegree PayDegreeType
	Diff float64
}

type PayDegreeType int

const (
	Low PayDegreeType = 0
	Just PayDegreeType = 1
	High PayDegreeType = 2
)

