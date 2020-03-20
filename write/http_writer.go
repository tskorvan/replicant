package write

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/spf13/viper"
)

// HTTPWriterPlugin - Plugin for writing operations to http output
type HTTPWriterPlugin struct {
}

func (h *HTTPWriterPlugin) Write(op Operation) error {
	var (
		err      error
		bytesOp  []byte
		response *http.Response
	)
	if bytesOp, err = json.Marshal(op); err != nil {
		return fmt.Errorf("Can't write to http output. Error: %w", err)
	}

	if response, err = http.Post(viper.GetString("write.http.url"), "application/json", bytes.NewBuffer(bytesOp)); err != nil {
		return fmt.Errorf("Can't write to http output. Error: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return fmt.Errorf("Can't write to http output. Http status: %d", response.StatusCode)
	}
	return nil
}

// Init - Initialization for HTTPWriterPlugin
func (h *HTTPWriterPlugin) Init() error {
	return nil
}
