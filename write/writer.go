package write

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// WriterPlugin - interface for writer plugin
type WriterPlugin interface {

	// Write operation throught writer plugin
	Write(Operation) error

	// Initialize writer plugin. Called once when NewWriter is created
	Init() error
}

// Writer - struct for writing data to multiple outputs
type Writer struct {
	Input   chan Operation
	writers []WriterPlugin
}

// NewWriter - create new instance of writer
func NewWriter() *Writer {
	writer := &Writer{
		Input:   make(chan Operation),
		writers: make([]WriterPlugin, 0),
	}
	writer.initPlugins()
	go writer.listen()
	return writer
}

func (w *Writer) initPlugins() {
	if viper.GetBool("write.stdOut.enable") {
		so := &StdOutWriterPlugin{}
		so.Init()
		w.writers = append(w.writers, so)
	}

	if viper.GetBool("write.http.enable") {
		htw := &HTTPWriterPlugin{}
		htw.Init()
		w.writers = append(w.writers, htw)
	}
}

// listen on chanel for input data
func (w *Writer) listen() {
	var err error
	for operation := range w.Input {
		for _, writer := range w.writers {
			if err = writer.Write(operation); err != nil {
				log.Error(err)
			}
		}
	}
}

// Close - close writer
func (w *Writer) Close() {
	close(w.Input)
	log.Info("Writer closed.")
}
