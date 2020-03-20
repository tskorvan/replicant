package main

import (
	"os"
	"os/signal"
	"syscall"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func init() {
	initConfiguration()
	initLogger()
}

func main() {
	r, err := NewReplicant()
	if err != nil {
		log.Error(err)
		return
	}
	defer r.Close()

	catchInterrupt(r)

	if err := r.Init(); err != nil {
		log.Error(err)
		return
	}

	go r.Listen()
	go r.Heartbeat()
	<-r.Done
}

func catchInterrupt(r *Replicant) {
	c := make(chan os.Signal)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func(r *Replicant) {
		<-c
		r.CtxCancel()
	}(r)
}

func initLogger() {
	log.SetLevel(log.Level(viper.GetInt("log.level")))
}

func initConfiguration() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal(err)
	}
}
