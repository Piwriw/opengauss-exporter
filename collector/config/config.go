// Copyright © 2020 Bin Liu <bin.liu@enmotech.com>

package config

import (
	"fmt"
	"golang.org/x/exp/slog"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

var MetricMap = make(map[string]*QueryInstance)

func InitConfig(configPath string) error {
	var err error
	MetricMap, err = LoadConfig(configPath)
	if err != nil {
		slog.Error("Error loading default configs.", slog.Any("error", err))
		return err
	}
	return nil
}

// LoadConfig 读取配置文件
func LoadConfig(configPath string) (queries map[string]*QueryInstance, err error) {
	stat, err := os.Stat(configPath)
	if err != nil {
		return nil, fmt.Errorf("invalid config path: %s: %w", configPath, err)
	}
	if stat.IsDir() { // recursively iterate conf files if a dir is given
		files, err := ioutil.ReadDir(configPath)
		if err != nil {
			return nil, fmt.Errorf("fail reading config dir: %s: %w", configPath, err)
		}
		slog.Info("load config from dir", slog.String("path", configPath))
		confFiles := make([]string, 0)
		for _, conf := range files {
			if !strings.HasSuffix(conf.Name(), ".yaml") && !conf.IsDir() { // depth = 1
				continue // skip non yaml files
			}
			confFiles = append(confFiles, path.Join(configPath, conf.Name()))
		}

		// make global config map and assign priority according to config file alphabetic orders
		// priority is an integer range from 1 to 999, where 1 - 99 is reserved for user
		queries = make(map[string]*QueryInstance)
		var queryCount, configCount int
		for _, confPath := range confFiles {
			if singleQueries, err := LoadConfig(confPath); err != nil {
				slog.Warn("skip config", slog.String("confPath", confPath), slog.Any("error", err))
			} else {
				configCount++
				for name, query := range singleQueries {
					queryCount++
					if query.Priority == 0 { // set to config rank if not manually set
						query.Priority = 100 + configCount
					}
					queries[name] = query // so the later one will overwrite former one
				}
			}
		}
		slog.Info("loaded config files", slog.Int("queries", len(queries)), slog.Int("queryCount", queryCount), slog.Int("configCount", configCount))
		return queries, nil
	}

	// single file case: recursive exit condition
	content, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("fail reading config file %s: %w", configPath, err)
	}
	queries, err = ParseConfig(content, stat.Name())
	if err != nil {
		return nil, err
	}
	slog.Info("loaded config files", slog.Int("queries", len(queries)), slog.String("configPath", configPath))
	return queries, nil
}

// ParseConfig turn config content into QueryInstance struct
func ParseConfig(content []byte, path string) (queries map[string]*QueryInstance, err error) {
	queries = make(map[string]*QueryInstance)
	if err = yaml.Unmarshal(content, &queries); err != nil {
		return nil, fmt.Errorf("malformed config: %w", err)
	}

	// parse additional fields
	for name, query := range queries {
		query.Path = path
		if query.Name == "" {
			query.Name = name
		}
		if err := query.Check(); err != nil {
			return nil, err
		}

	}
	return
}
