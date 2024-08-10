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

type pgStatBgwriterCollector struct {
	logger log.Logger
	db     *sql.DB
}

func (p *pgStatBgwriterCollector) name() string {
	return "pg_stat_bgwriter"
}

func (p *pgStatBgwriterCollector) Update(ch chan<- prometheus.Metric) error {
	queryInstance, ok := config.MetricMap[p.name()]
	if !ok {
		return fmt.Errorf("can not find pg_stat_bgwriter from MetricMap")
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
	collector.RegisterCollector("pg_stat_bgwriter", collector.DefaultEnabled, NewpgStatBgwriterCollector)
}

func NewpgStatBgwriterCollector(logger log.Logger) (collector.Collector, error) {
	//db, err := gorm.Open(postgres.Open("postgresql://gaussdb:Enmo@123@47.107.113.111:15432/postgres"), &gorm.Config{})

	return &pgStatBgwriterCollector{
		db:     config.GetDBConnection(config.MonitDB.Address, config.MonitDB.Port),
		logger: logger,
	}, nil
}
