package config

import "database/sql"

var MonitDB = &GaussDBConnectConfig{}

type GaussDBConnectConfig struct {
	Address  string `json:"address"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password"`
	Database string `json:"database"`
}

type GBInfo struct {
	Version              string `json:"version"`
	Connection           *sql.DB
	GaussDBConnectConfig *GaussDBConnectConfig
}
