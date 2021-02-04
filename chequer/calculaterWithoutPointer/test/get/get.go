package get

import "fmt"

func Get(s *string) string{
	fmt.Scan(s)
	return *s
}
