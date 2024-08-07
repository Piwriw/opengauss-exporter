package opengauss

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-kit/log"
	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/node_exporter/collector"
	"github.com/prometheus/node_exporter/collector/config"
)

type pgDataBaseCollector struct {
	logger log.Logger
	db     *sql.DB
}

func (p *pgDataBaseCollector) name() string {
	return "pg_database"
}

func (p *pgDataBaseCollector) Update(ch chan<- prometheus.Metric) error {
	queryInstance, ok := config.MetricMap[p.name()]
	if !ok {
		return fmt.Errorf("ccan not find pg_database from MetricMap")
	}

	if err := queryInstance.Check(); err != nil {
		return err
	}
	metrics := getMetric(context.TODO(), p.db, queryInstance)
	for _, metric := range metrics {
		ch <- metric
	}
	return nil
}

func init() {
	collector.RegisterCollector("pg_database", collector.DefaultEnabled, NewPGDataBaseCollector)
}

func NewPGDataBaseCollector(logger log.Logger) (collector.Collector, error) {
	//db, err := gorm.Open(postgres.Open("postgresql://gaussdb:Enmo@123@47.107.113.111:15432/postgres"), &gorm.Config{})

	db, err := sql.Open("postgres", "postgresql://gaussdb:Enmo@123@47.107.113.111:15432/postgres?sslmode=disable")
	if err != nil {
		return nil, err
	}

	return &pgDataBaseCollector{
		db:     db,
		logger: logger,
	}, nil
}
