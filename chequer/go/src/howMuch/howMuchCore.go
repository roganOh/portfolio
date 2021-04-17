package howMuch

import "fmt"

func Calculate(friends []People, costAvg float64) {
	for len(friends) > 1 {
		f := friends[0]
		if -f.Diff > friends[len(friends)-1].Diff { //다 채워준다
			fmt.Print(f.Name + " -> " + friends[len(friends)-1].Name + "   " + fmt.Sprintf("%f", friends[len(friends)-1].Diff) + " 주기")
			fmt.Print("\n")
			friends = friends[:len(friends)-1]
			friends[0].Money += friends[len(friends)-1].Diff
			friends[0].Diff = friends[0].Money - costAvg
		} else { //다 채워주지 못한다
			fmt.Print(f.Name + " -> " + friends[len(friends)-1].Name + "   " + fmt.Sprintf("%f", -f.Diff) + " 주기")
			fmt.Print("\n")
			friends = friends[1:]
			friends[len(friends)-1].Money -= friends[0].Diff
			friends[len(friends)-1].Diff = friends[len(friends)-1].Money - costAvg
		}
	}

}
