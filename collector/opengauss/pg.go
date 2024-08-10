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

type pgCollector struct {
	logger  log.Logger
	up      prometheus.Gauge
	version prometheus.Gauge
	db      *sql.DB
}

func (p *pgCollector) name() string {
	return "pg"
}

func (p *pgCollector) Update(ch chan<- prometheus.Metric) error {
	queryInstance, ok := config.MetricMap[p.name()]
	if !ok {
		return fmt.Errorf("can not find pg from MetricMap")
	}

	if err := queryInstance.Check(); err != nil {
		return err
	}
	metrics := getMetric(context.TODO(), p.db, queryInstance)
	for _, metric := range metrics {
		ch <- metric
	}
	p.up.Set(1)
	ch <- p.up
	dbVersion := config.GetDBVersion(config.MonitDB.Address, config.MonitDB.Port)
	if dbVersion == "" {
		return fmt.Errorf("can not get version information")
	}
	p.version = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "pg", ConstLabels: prometheus.Labels{"server": fmt.Sprintf("%s:%d", config.MonitDB.Address, config.MonitDB.Port), "short_version": dbVersion},
		Name: "version", Help: "get version information",
	})
	ch <- p.version
	return nil
}

func init() {
	collector.RegisterCollector("pg", collector.DefaultEnabled, NewpgCollector)
}

func NewpgCollector(logger log.Logger) (collector.Collector, error) {
	//db, err := gorm.Open(postgres.Open("postgresql://gaussdb:Enmo@123@47.107.113.111:15432/postgres"), &gorm.Config{})

	return &pgCollector{
		up: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "pg", ConstLabels: prometheus.Labels{"server": fmt.Sprintf("%s:%d", config.MonitDB.Address, config.MonitDB.Port)},
			Name: "up", Help: "always be 1 if your could retrieve metrics",
		}),
		db:     config.GetDBConnection(config.MonitDB.Address, config.MonitDB.Port),
		logger: logger,
	}, nil
}
