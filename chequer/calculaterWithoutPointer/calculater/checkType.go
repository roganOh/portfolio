package calculater

import "strconv"

func IsDigit(ch string) bool {
	_, err := strconv.ParseFloat(ch, 64)
	return err == nil
}

func IsNotDigit(ch string) bool {
	return IsDigit(ch) == false
}

func TypeDefine(ch string) string {
	if IsDigit(ch) {
		return "num"
	} else {
		switch ch {
		case "_":
			return "minus"
		case "+", "-", "*", "/":
			return "operater"
		case "[", "{", "(":
			return "openbrace"
		case "]", "}", ")":
			return "closebrace"
		case ".":
			return "dot"
		}
	}
	return "unknown"
}

func groupingNumbers(eqn string) []string {
	var state int
	var num string
	var sequence int
	inFix := make([]string, len(eqn))

	for i, v := range eqn {
		ch := string(v)
		switch state {
		case -1:
			switch TypeDefine(ch) {
			case "num":
				state = 1
				num += ch
			default:
				ErrorWithWhere(eqn, "only number can come after minus", i)
			}
		case 0:
			switch TypeDefine(ch) {
			case "num":
				state = 1
				num += ch
			case "minus":
				state = -1
				num += ch
			case "dot":
				ErrorWithWhere(eqn, "dot can't come after number or operater", i)
			case "closebrace":
				ErrorWithWhere(eqn, "close brace can't come after operater", i)
			case "operater":
				inFix[sequence] = ch
				sequence++
			case "openbrace":
				if sequence > 0 {
					if TypeDefine(inFix[sequence]) == "operater" {
						inFix[sequence] = ch
						sequence++
					} else {
						ErrorWithWhere(eqn, "open brace error", i)
					}
				} else {
					inFix[sequence] = ch
					sequence++
				}

			}
		case 1:
			switch TypeDefine(ch) {
			case "num":
				state = 2
				num += ch
			case "minus":
				ErrorWithWhere(eqn, "minus can't come right after number", i)
			case "operater", "closebrace":
				inFix[sequence] = num
				sequence++
				inFix[sequence] = ch
				sequence++
				state = 0
				num = ""
			case "openbrace":
				ErrorWithWhere(eqn, "open brace can't come right after number", i)
			case "dot":
				num += ch
				state = 3
			}
		case 2:
			switch TypeDefine(ch) {
			case "num":
				num += ch
			case "minus":
				ErrorWithWhere(eqn, "minus can't come right after number", i)
			case "operater", "closebrace":
				inFix[sequence] = num
				sequence++
				inFix[sequence] = ch
				sequence++
				state = 0
				num = ""
			case "openbrace":
				ErrorWithWhere(eqn, "open brace can't come right after number", i)
			case "dot":
				num += ch
				state = 3
			}
		case 3:
			switch TypeDefine(ch) {
			case "num":
				num += ch
				state = 4
			case "minus":
				ErrorWithWhere(eqn, "minus can't come right after number", i)
			case "operater":
				ErrorWithWhere(eqn, "operater can't ome in decimal number", i)
			case "openbrace", "closebrace":
				ErrorWithWhere(eqn, "brace can't come right after number", i)
			case "dot":
				ErrorWithWhere(eqn, "dot can't come in decimal number", i)
			}
		case 4:
			switch TypeDefine(ch) {
			case "num":
				num += ch
			case "minus":
				ErrorWithWhere(eqn, "minus can't come right after number", i)
			case "operater", "closebrace":
				inFix[sequence] = num
				sequence++
				inFix[sequence] = ch
				sequence++
				state = 0
				num = ""
			case "openbrace":
				ErrorWithWhere(eqn, "open brace can't come right after number", i)
			case "dot":
				ErrorWithWhere(eqn, "dot can't come in decimal number", i)
			}

		}

	}
	if num != "" {
		inFix[sequence] = num
	}
	return inFix
}

func MakeStringToStructStackWithType(eqn string, inFixList ValueNType) {
	equationList := groupingNumbers(eqn)
	for i, v := range equationList {
		inFixList[i].V = v
		inFixList[i].T = TypeDefine(v)
		i++
	}
}
