package main

import (
	"fmt"
	"math/rand"
	"reflect"
	"time"
)

type DiscordianErisian struct {
	Name   string
	Number int
}

func (de DiscordianErisian) LawOfFives() bool {
	return de.Number%5 == 0
}
func (de DiscordianErisian) String() string {
	if de.LawOfFives() {
		return fmt.Sprintf("Hail Eris! The number %d is sacred because it's a multiple of 5!", de.Number)
	}
	return fmt.Sprintf("%d is just mundane.", de.Number)
}

func GenerateRandomErisian() DiscordianErisian {
	rand.Seed(time.Now().UnixNano())
	return DiscordianErisian{Name: "Random Erisian", Number: rand.Intn(100)}
}

func main() {
	var erisian DiscordianErisian
	for i := 0; i < 5; i++ {
		erisian = GenerateRandomErisian()
		fmt.Println(erisian)
	}

	reflection := reflect.ValueOf(erisian)
	fmt.Println("Reflecting upon the Erisian nature:", reflection)
}
