package main

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

func init() {
	initConfiguration()
	initLogger()
}

func main() {
	r := NewReplicant()
	defer r.Close()

	// c := make(chan os.Signal)
	// signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	// go func(r *Replicant) {
	// 	log.Info("waiting for unterrupt")
	// 	<-c
	// 	r.Close()
	// 	os.Exit(1)
	// }(r)

	if err := r.Init(); err != nil {
		log.Error(err)
		return
	}

	go r.Listen()
	go r.Heartbeat()
	<-r.Done
}

func catchInterrupt(r *Replicant) {

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
