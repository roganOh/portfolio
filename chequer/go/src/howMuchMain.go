package main

import (
	"sort"
)
import "howMuch"

func main() {
	var friends []howMuch.People
	var costAvg float64
	friends = howMuch.GetPeople()
	costAvg = howMuch.GetMoneyNAvg(friends)
	howMuch.SetPayDegree(friends, costAvg)
	sort.Slice(friends, func(i, j int) bool {
		return friends[i].Money < friends[j].Money
	})
	print("=====================================================================\n")
	howMuch.Calculate(friends, costAvg)
}
