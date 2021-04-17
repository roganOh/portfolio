package calculater

func GetCorrectInFixList() ValueNType {
	var eqn string
	for NoErr := false; true; eqn = "" {
		inFixList := make(ValueNType, 0)
		println("enter your equation")
		GetCorrectEqn(&eqn)
		NoErr, inFixList = MakeInFixWithMappingType(eqn, inFixList)
		if NoErr {
			return inFixList
		}
	}
	return []Element{}
}
