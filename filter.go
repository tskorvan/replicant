package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	log "github.com/sirupsen/logrus"
	"github.com/tskorvan/replicant/write"
)

type table struct {
	childSchema  string
	childTable   string
	parentSchema string
	parentTable  string
}

// Filter - filter replicated data throught filter.json file. Also transform table names to parent (partitioned tables)
type Filter struct {
	Input   chan write.Operations
	Writer  *write.Writer
	allowed map[string]bool
	tables  map[string]*table
	db      *Database
}

// NewFilter - creates new filter, initialize it and start listening on input data chanel
func NewFilter(database *Database) (*Filter, error) {
	w := write.NewWriter()
	f := &Filter{
		Input:   make(chan write.Operations),
		Writer:  w,
		allowed: nil,
		tables:  nil,
		db:      database,
	}
	f.loadAllowed()
	if err := f.loadTableMap(); err != nil {
		return nil, err
	}
	return f, nil
}

// Listen - Listen on filter input chanel
func (f *Filter) Listen() {
	for {
		select {
		case value := <-f.Input:
			f.filter(value)
		}
	}
}

// filter input operations
func (f *Filter) filter(operations write.Operations) {
	var parentTable string
	for _, operation := range operations {
		table, exist := f.tables[operation.Schema+"."+operation.Table]
		if exist {
			parentTable = table.parentSchema + "." + table.parentTable
		} else {
			log.Error("Can't find table " + operation.Schema + "." + operation.Table + " in table map.")
			continue
		}

		if _, exist := f.allowed[parentTable]; f.allowed == nil || exist {
			operation.Schema = table.parentSchema
			operation.Table = table.parentTable
			f.Writer.Input <- operation
		}
	}

}

// Load allowed tables from filter.json
func (f *Filter) loadAllowed() {
	allowedBytes, err := ioutil.ReadFile("./filter.json")
	if err != nil {
		log.Warningf("Can't load allowed filter, all data passes throught filter without filtering. Err: %v", err)
		return
	}

	allowed := make([]struct {
		Schema string
		Table  string
	}, 0)

	if err = json.Unmarshal(allowedBytes, &allowed); err != nil {
		log.Warningf("Can't parse allowed filter, all data passes throught filter without filtering. Err: %v", err)
		return
	}

	f.allowed = make(map[string]bool)
	for _, a := range allowed {
		f.allowed[a.Schema+"."+a.Table] = true
	}
}

// Load partitioned table mapping
func (f *Filter) loadTableMap() error {
	rows, err := f.db.Query(`
	select 
		nsp.nspname as childSchema, 
		cls.relname as childTable,
		case when inh.inhrelid is null then nsp.nspname else parentnsp.nspname end as parentSchema,
		case when inh.inhrelid is null then cls.relname else parent.relname end as parentTable
	from 
		pg_class cls
		inner join pg_namespace nsp on nsp.oid = cls.relnamespace
		left join pg_inherits inh on inh.inhrelid = cls.oid
		left join pg_class parent on parent.oid = inh.inhparent
		left join pg_namespace parentnsp on parentnsp.oid = parent.relnamespace
	where
		cls.relkind = 'r' and nsp.nspname not in ('information_schema', 'pg_catalog')
	order by 1
	`)
	if err != nil {
		return fmt.Errorf("Can't load table map: %v", err)
	}

	f.tables = make(map[string]*table)
	for rows.Next() {
		var childSchema, childTable, parentSchema, parentTable string
		if err = rows.Scan(&childSchema, &childTable, &parentSchema, &parentTable); err != nil {
			return err
		}
		f.tables[childSchema+"."+childTable] = &table{
			childSchema:  childSchema,
			childTable:   childTable,
			parentSchema: parentSchema,
			parentTable:  parentTable,
		}
	}
	defer rows.Close()
	return rows.Err()
}

func (f *Filter) Close() {
	f.Writer.Close()
	close(f.Input)
}
