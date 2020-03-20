package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/tskorvan/replicant/write"

	"github.com/jackc/pgx"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Replicant - main replicant struc
type Replicant struct {
	db          *Database
	filter      *Filter
	actualPoint uint64
	ctx         context.Context
	CtxCancel   context.CancelFunc
	Done        chan bool
}

// NewReplicant - Create Replicant instance
func NewReplicant() (*Replicant, error) {
	ctx, ctxCancel := context.WithCancel(context.Background())
	database := NewDatabase()
	filter, err := NewFilter(database)
	if err != nil {
		ctxCancel()
		return nil, err
	}
	return &Replicant{
		db:        database,
		filter:    filter,
		ctx:       ctx,
		CtxCancel: ctxCancel,
		Done:      make(chan bool),
	}, nil
}

// Init - Initialize replicant. Create replication slot and start replication
func (r *Replicant) Init() error {
	slotName := viper.GetString("database.replication-slot-name")

	if r.existReplicationSlot() {
		if err := r.db.DropReplicationSlot(slotName); err != nil {
			log.Fatal("Can't drop replication slot: ", err)
		}
		log.Infof("Replication slot %v closed.", slotName)
	}

	point, _, err := r.db.CreateReplicationSlotEx(slotName, "wal2json")
	if err != nil {
		return fmt.Errorf("Can't create replication slot: %v", err)
	}
	log.Infof("Replication slot %v created", slotName)

	if r.actualPoint, err = pgx.ParseLSN(point); err != nil {
		return fmt.Errorf("Can't parse last wal log position %s: %v", point, err)
	}

	if err = r.db.StartReplication(slotName, r.actualPoint, -1); err != nil {
		return fmt.Errorf("Can't start replication: %v", err)
	}
	log.Infof("Replication start at point %v", point)
	return nil
}

// Close - Close replicant, drop replication slot and close database connection
func (r *Replicant) Close() {
	r.printWalPosition()
	r.filter.Close()
	r.db.Close()
	log.Info("Replicant closed.")
}

// Listen - Listen for replication messages
func (r *Replicant) Listen() {
	var (
		err     error
		message *pgx.ReplicationMessage
	)

	go r.filter.Listen()

	change := new(struct {
		Change write.Operations
	})
	for {
		log.Debug("Waiting for WAL message.")
		message, err = r.db.WaitForReplicationMessage(r.ctx)
		if err != nil {
			log.Error("Can't get replication message:", err)
			r.Done <- true
			return
		}
		if message.WalMessage != nil {
			if err = json.Unmarshal(message.WalMessage.WalData, change); err != nil {
				log.Errorf("Can't unmarshal json WAL message: %v", string(message.WalMessage.WalData))
				continue
			}
			if len(change.Change) > 0 {
				r.filter.Input <- change.Change
			}
		}
		if message.ServerHeartbeat != nil {
			log.Debugf("Got HeartBeat message %+v", message.ServerHeartbeat)
			if message.ServerHeartbeat.ReplyRequested == 1 {
				if err = r.sendHeartbeat(); err != nil {
					log.Error(err)
				}
			}
		}
	}
}

// Heartbeat - Periodicaly sending heartbeat message to PostgreSQL server
func (r *Replicant) Heartbeat() {
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-time.Tick(time.Duration(10) * time.Second):
			{
				if err := r.sendHeartbeat(); err != nil {
					log.Error(err)
				}
			}
		}
	}
}

func (r *Replicant) sendHeartbeat() error {
	heartBeatMsg, err := pgx.NewStandbyStatus(r.actualPoint)
	if err != nil {
		return fmt.Errorf("Can't create heartbeat message: %v", err)
	}
	heartBeatMsg.ReplyRequested = 0
	if err = r.db.SendStandbyStatus(heartBeatMsg); err != nil {
		return fmt.Errorf("Can't send heartbeat message: %v", err)
	}
	return nil
}

// Check if replication slot exists
func (r *Replicant) existReplicationSlot() bool {
	var exists int
	err := r.db.QueryRow("SELECT 1 FROM pg_replication_slots WHERE slot_name = $1", viper.GetString("database.replication-slot-name")).Scan(&exists)
	switch err {
	case sql.ErrNoRows:
		return false
	case nil:
		return true
	default:
		log.Error(err)
		return false
	}
}

func (r *Replicant) printWalPosition() {
	//TODO
}
