package write

import (
	"github.com/spf13/viper"
)

type IWriter interface {
	Write(Operation)
	Init() error
}

type Writer struct {
	Input   chan Operation
	writers []IWriter
}

func NewWriter() *Writer {
	writer := &Writer{
		Input:   make(chan Operation),
		writers: make([]IWriter, 0),
	}

	if viper.GetBool("write.stdOut.enable") {
		so := &StdOutWriter{}
		so.Init()
		writer.writers = append(writer.writers, so)
	}

	if viper.GetBool("write.http.enable") {
		htw := &HttpWriter{}
		htw.Init()
		writer.writers = append(writer.writers, htw)
	}

	go writer.listen()
	return writer
}

func (w *Writer) listen() {
	for operation := range w.Input {
		for _, writer := range w.writers {
			writer.Write(operation)
		}
	}
}

func (w *Writer) Close() {
	close(w.Input)
}
