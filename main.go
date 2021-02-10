package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	var once sync.Once
	for i, v := range make([]string, 10) {
		once.Do(onces)
		fmt.Println("count:", v, "---", i)
	}
	for i := 0; i < 10; i++ {
		go func() {
			once.Do(onced)
			fmt.Println(123)
		}()
	}
	time.Sleep(3000)
}
func onces() {
	fmt.Println("onces")
}
func onced() {
	fmt.Println("onced")
}
