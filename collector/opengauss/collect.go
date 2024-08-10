package opengauss

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/node_exporter/collector/config"
	"github.com/prometheus/node_exporter/collector/utils"
	"golang.org/x/exp/slog"
	"strings"
	"unicode/utf8"
)

func getMetric(ctx context.Context, db *sql.DB, queryInstance *config.QueryInstance) []prometheus.Metric {
	columnNames := make([]string, 0)
	var list [][]interface{}

	for _, query := range queryInstance.Queries {
		if query.Status == "disable" {
			continue
		}
		rows, err := db.QueryContext(ctx, query.SQL)
		if err != nil {
			slog.Error("db Query is failed", slog.Any("err", err))
			continue
		}
		if rows == nil {
			slog.Warn("rows is empty")
			continue
		}
		columnNames, err = rows.Columns()
		for rows.Next() {
			var columnData = make([]interface{}, len(columnNames))
			var scanArgs = make([]interface{}, len(columnNames))
			for i := range columnData {
				scanArgs[i] = &columnData[i]
			}
			err = rows.Scan(scanArgs...)
			if err != nil {
				slog.Error("errr ")
				break
			}
			list = append(list, columnData)
		}
	}
	// Make a lookup map for the column indices
	var columnIdx = make(map[string]int, len(columnNames))
	for i, n := range columnNames {
		columnIdx[n] = i
	}
	metrics := make([]prometheus.Metric, 0)
	for i := range list {
		metric, errs := procRows(queryInstance, columnNames, columnIdx, list[i])
		if len(errs) > 0 {
			//nonfatalErrors = append(nonfatalErrors, errs...)
		}
		if metric != nil {
			metrics = append(metrics, metric...)
		}
	}
	return metrics
}
func procRows(queryInstance *config.QueryInstance, columnNames []string, columnIdx map[string]int, columnData []interface{}) ([]prometheus.Metric, []error) {
	// Get the label values for this row.
	metrics := make([]prometheus.Metric, 0)
	nonfatalErrors := []error{}
	labels := make([]string, len(queryInstance.LabelNames))
	var dbName string
	dbNameLabel := queryInstance.DBNameLabel
	if dbNameLabel != "" {
		dbName, _ = utils.DbToString(columnData[columnIdx[dbNameLabel]], true)
	}
	for idx, label := range queryInstance.LabelNames {
		v, err := decode(queryInstance, columnData[columnIdx[label]], label, dbName)
		if err != nil {
			slog.Error("decode error", slog.Any("err", err))
		}
		labels[idx] = v
	}
	// Loop over column names, and match to scan data. Unknown columns
	// will be filled with an untyped metric number *if* they can be
	// converted to float64s. NULLs are allowed and treated as NaN.
	for idx, columnName := range columnNames {
		//col := queryInstance.GetColumn(columnName, s.labels)
		col := queryInstance.GetColumn(columnName, prometheus.Labels{"server": fmt.Sprintf("%s:%d", config.MonitDB.Address, config.MonitDB.Port)})
		metric, err := newMetric(queryInstance, col, columnName, columnData[idx], labels)
		if err != nil {
			slog.Error("newMetric", slog.Any("err", err))
			nonfatalErrors = append(nonfatalErrors, err)
			continue
		}
		if metric != nil {
			metrics = append(metrics, metric)
		}
	}
	return metrics, nonfatalErrors
}

func newMetric(queryInstance *config.QueryInstance, col *config.Column, columnName string, colValue interface{},
	labels []string) (metric prometheus.Metric, err error) {
	var (
		desc       *prometheus.Desc
		value      float64
		valueOK    bool
		metricName = queryInstance.Name
		valueType  prometheus.ValueType
	)
	if col == nil {
		return nil, nil
	}
	if col.DisCard {
		return nil, nil
	}
	if col.Histogram {
		return nil, nil
	}
	if strings.EqualFold(col.Usage, config.MappedMETRIC) {
		return nil, nil
	}
	desc = col.PrometheusDesc
	valueType = col.PrometheusType
	value, valueOK = utils.DbToFloat64(colValue)
	if !valueOK {
		return nil, errors.New(fmt.Sprintln("Unexpected error parsing column: ", metricName, columnName, colValue))
	}
	defer utils.RecoverErr(&err)
	metric = prometheus.MustNewConstMetric(desc, valueType, value, labels...)
	return metric, nil
}

func decode(queryInstance *config.QueryInstance, data interface{}, label, dbName string) (string, error) {
	v, _ := utils.DbToString(data, false)
	col := queryInstance.GetColumn(label, prometheus.Labels{"server": fmt.Sprintf("%s:%d", config.MonitDB.Address, config.MonitDB.Port)})
	if col == nil {
		return v, nil
	}
	if !col.CheckUTF8 {
		return v, nil
	}
	if utf8.ValidString(v) {
		return v, nil
	}
	//// 检查编码是否UTF8,不是则改为空
	//if s.dbInfoMap == nil {
	//	return "", nil
	//}
	if dbName == "" {
		return "", nil
	}
	//dbInfo, ok := s.dbInfoMap[dbName]
	//if !ok {
	//	return "", nil
	//}
	//if dbInfo == nil {
	//	return "", nil
	//}
	//if dbInfo.Charset == "" {
	//	return "", nil
	//}
	//if s.clientEncoding == UTF8 && dbInfo.Charset ==  {
	//	return "", nil
	//}
	b, err := utils.DecodeByte([]byte(v), "UTF8")
	if err != nil {
		slog.Info("DecodeByte", slog.Any("err", err))
		return "", nil
	}
	return string(b), nil

}
