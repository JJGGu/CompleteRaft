package main

import (
	"fmt"
	"time"
)

func main() {
	timer := time.NewTimer(2 * time.Second)
	t1 := time.Now()
	t2 := <-timer.C
	fmt.Println(t1)
	fmt.Println(t2)
}
