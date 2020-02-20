package main

import (
	"encoding/json"
	"io/ioutil"

	log "github.com/sirupsen/logrus"
)

type Filter struct {
	Input   chan Operations
	Output  chan string
	allowed map[string]bool
}

func NewFilter() *Filter {
	f := &Filter{
		Input:   make(chan Operations),
		Output:  make(chan string),
		allowed: nil,
	}
	f.loadAllowed()
	go f.listen()
	return f
}

func (f *Filter) listen() {
	for {
		select {
		case value := <-f.Input:
			f.filter(value)
		}
	}
}

func (f *Filter) filter(operations Operations) {
	for _, operation := range operations {
		if _, exist := f.allowed[operation.Schema+"."+operation.Table]; f.allowed == nil || exist {
			log.Info("Filtered Table: ", operation.Table)
		}
	}

}

func (f *Filter) loadAllowed() {
	allowedBytes, err := ioutil.ReadFile("./filter.json")
	if err != nil {
		log.Error("Can't load allowed filter: ", err)
		return
	}

	allowed := make([]struct {
		Schema string
		Table  string
	}, 0)

	if err = json.Unmarshal(allowedBytes, &allowed); err != nil {
		log.Error("Can't parse allowed filter: ", err)
		return
	}

	f.allowed = make(map[string]bool)
	for _, a := range allowed {
		f.allowed[a.Schema+"."+a.Table] = true
	}
}
