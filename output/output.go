package output

// Writer - interface for output plugins
type Writer interface {
	Write() error
	Init() error
}
