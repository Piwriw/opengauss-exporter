package opengauss

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/node_exporter/collector"
	"github.com/prometheus/node_exporter/collector/config"
)

type pgStatReplicationCollector struct {
	logger log.Logger
	db     *sql.DB
}

func (p *pgStatReplicationCollector) name() string {
	return "pg_stat_replication"
}

func (p *pgStatReplicationCollector) Update(ch chan<- prometheus.Metric) error {
	queryInstance, ok := config.MetricMap[p.name()]
	if !ok {
		return fmt.Errorf("can not find pg_stat_replication from MetricMap")
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
	collector.RegisterCollector("pg_stat_replication", collector.DefaultEnabled, NewpgStatReplicationCollector)
}

func NewpgStatReplicationCollector(logger log.Logger) (collector.Collector, error) {
	return &pgStatReplicationCollector{
		db:     config.GetDBConnection(config.MonitDB.Address, config.MonitDB.Port),
		logger: logger,
	}, nil
}
