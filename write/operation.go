package write

// Operation - structure for database operations, wall2json format
type Operation struct {
	Schema       string
	Table        string
	Columnnames  []string
	Columntypes  []string
	Columnvalues []interface{}
}

// Operations - Array of Operation structs
type Operations []Operation
