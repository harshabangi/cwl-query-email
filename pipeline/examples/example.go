package main

import (
	"fmt"
	"github.com/harshabangi/go-concurrency/pipeline/v1"
	"log"
)

func main() {
	p := pipeline.New()

	p.AddStage(1, func(input interface{}) (interface{}, error) {
		i := input.(int)
		return i + 1, nil
	})

	p.AddStage(2, func(input interface{}) (interface{}, error) {
		i := input.(int)
		return i + 1, nil
	})

	p.AddStage(3, func(input interface{}) (interface{}, error) {
		i := input.(int)
		return i + 1, nil
	})

	out, err := p.Run([]interface{}{1, 2, 3})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(out)
}
