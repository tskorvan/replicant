package write

import "fmt"

type HttpWriter struct {
}

func (h *HttpWriter) Write(op Operation) {
	fmt.Println("HTTP:", op)
}

func (h *HttpWriter) Init() error {
	return nil
}
