package write

import "fmt"

type StdOutWriter struct {
}

func (so *StdOutWriter) Write(op Operation) {
	fmt.Println(op)
}

func (so *StdOutWriter) Init() error {
	return nil
}
