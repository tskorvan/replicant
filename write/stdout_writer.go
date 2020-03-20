package write

import "fmt"

// StdOutWriterPlugin - Plugin for writing to stanard output
type StdOutWriterPlugin struct {
}

func (so *StdOutWriterPlugin) Write(op Operation) error {
	if _, err := fmt.Println(op); err != nil {
		return fmt.Errorf("Can't write to std out. Error: %v", err)
	}
	return nil
}

// Init - Initialization for StdOutWriterPlugin
func (so *StdOutWriterPlugin) Init() error {
	return nil
}
