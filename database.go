package main

import (
	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Database - Replication connection to database
type Database struct {
	*pgx.ReplicationConn
}

// NewDatabase - Returns new database connection
func NewDatabase() *Database {
	return &Database{getOpenConnection()}
}

func getOpenConnection() *pgx.ReplicationConn {
	connection, err := pgx.ReplicationConnect(pgx.ConnConfig{
		Host:     viper.GetString("database.host"),
		Port:     uint16(viper.GetUint("database.port")),
		Database: viper.GetString("database.name"),
		User:     viper.GetString("database.user"),
		Password: viper.GetString("database.password"),
	})
	if err != nil {
		log.Fatal("Can't connect to database: ", err)
	}
	return connection
}
