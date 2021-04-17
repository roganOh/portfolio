package howMuch

import (
	"calculater"
	"fmt"
	"strconv"
)

func GetPeople() []People {
	var friends []People
	var friend = People{}
	for {
		_, _ = fmt.Scan(&friend.Name)
		if friend.Name == "끝" {
			break
		}
		friends = append(friends, friend)
	}
	return friends
}

func GetMoneyNAvg(friends []People) float64 {
	var postFixList calculater.ValueNType
	var money float64
	var result string
	var costTotal float64
	var costAvg float64

	for i, f := range friends {
		for continues := true; continues; calculater.CheckIfUserContinue(&continues) {
			inFixList := calculater.GetCorrectInFixList()
			fmt.Println(inFixList)
			postFixList = inFixList.InfixToPostfix()
			fmt.Println(postFixList)
			result = postFixList.Calculate()
		}
		money, _ = strconv.ParseFloat(result, 64)
		friends[i].Money = money
		costTotal += money
		print(f.Name + ": " + result)
		print("\n")
	}
	costAvg = costTotal / float64(len(friends))
	return costAvg
}

func SetPayDegree(friends []People, costAvg float64) {
	for i, f := range friends {
		friends[i].Diff = f.Money - costAvg
		switch {
		case friends[i].Diff < 0:
			friends[i].PayDegree = Low
		case friends[i].Diff == 0:
			friends[i].PayDegree = Just
			friends = append(friends[:i-1], friends[i:]...)
		case friends[i].Diff > 0:
			friends[i].PayDegree = High
		}
	}
}
