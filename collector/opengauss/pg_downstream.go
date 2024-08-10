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

type pgDownStreamCollector struct {
	logger log.Logger
	db     *sql.DB
}

func (p *pgDownStreamCollector) name() string {
	return "pg_downstream"
}

func (p *pgDownStreamCollector) Update(ch chan<- prometheus.Metric) error {
	queryInstance, ok := config.MetricMap[p.name()]
	if !ok {
		return fmt.Errorf("can not find pgDownStream from MetricMap")
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
	collector.RegisterCollector("pg_downstream", collector.DefaultEnabled, NewpgDownStreamCollector)
}

func NewpgDownStreamCollector(logger log.Logger) (collector.Collector, error) {

	return &pgDownStreamCollector{
		db:     config.GetDBConnection(config.MonitDB.Address, config.MonitDB.Port),
		logger: logger,
	}, nil
}
