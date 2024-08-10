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

type gsSessionMemoryDetailCollector struct {
	logger log.Logger
	db     *sql.DB
}

func (p *gsSessionMemoryDetailCollector) name() string {
	return "gs_session_memory_detail"
}

func (p *gsSessionMemoryDetailCollector) Update(ch chan<- prometheus.Metric) error {
	queryInstance, ok := config.MetricMap[p.name()]
	if !ok {
		return fmt.Errorf("can not find gs_session_memory_detail from MetricMap")
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
	collector.RegisterCollector("gs_session_memory_detail", collector.DefaultEnabled, NewgsSessionMemoryDetailCollector)
}

func NewgsSessionMemoryDetailCollector(logger log.Logger) (collector.Collector, error) {
	//db, err := gorm.Open(postgres.Open("postgresql://gaussdb:Enmo@123@47.107.113.111:15432/postgres"), &gorm.Config{})

	return &gsSessionMemoryDetailCollector{
		db:     config.GetDBConnection(config.MonitDB.Address, config.MonitDB.Port),
		logger: logger,
	}, nil
}
