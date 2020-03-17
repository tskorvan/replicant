package write

type Operation struct {
	Schema       string
	Table        string
	Columnnames  []string
	Columntypes  []string
	Columnvalues []interface{}
}

type Operations []Operation
