// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"github.com/prometheus/common/version"
	"github.com/prometheus/node_exporter/collector/config"
	_ "github.com/prometheus/node_exporter/collector/opengauss"
	"golang.org/x/exp/slog"
	stdlog "log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"os/user"
	"runtime"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/common/promlog"
	"github.com/prometheus/common/promlog/flag"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	promcollectors "github.com/prometheus/client_golang/prometheus/collectors"
	versioncollector "github.com/prometheus/client_golang/prometheus/collectors/version"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/node_exporter/collector"
	np "net/http/pprof"
)

var (
	ReloadLock sync.Mutex
	args       = &Args{}
)

// Args General generic options
type Args struct {
	Help                   *bool   `short:"h" long:"help" description:"Displays help info"`
	Version                *bool   `short:"v" long:"version" description:"Displays mtk version"`
	DbURL                  *string `short:"d" long:"url" description:"openGauss database target url" env:"OG_EXPORTER_URL"`
	ConfigPath             *string `short:"c" long:"config" description:"path to config dir or file" env:"OG_EXPORTER_CONFIG"`
	ConstLabels            *string `short:"l" long:"label" description:"constant lables:comma separated list of label=value pair" env:"OG_EXPORTER_LABEL"`
	ServerTags             *string `short:"t" long:"tags" description:"tags,comma separated list of server tag" env:"OG_EXPORTER_TAG"`
	DisableCache           *bool   `long:"disable-cache" description:"force not using cache" env:"OG_EXPORTER_DISABLE_CACHE"`
	AutoDiscovery          *bool   `long:"auto-discovery" description:"automatically scrape all database for given server" env:"OG_EXPORTER_AUTO_DISCOVERY"`
	ExcludeDatabase        *string `long:"exclude-database" description:"excluded databases when enabling auto-discovery" default:"template0,template1" env:"OG_EXPORTER_EXCLUDE_DATABASE"`
	IncludeDatabase        *string
	ExporterNamespace      *string `long:"namespace" description:"prefix of built-in metrics, (og) by default" env:"OG_EXPORTER_NAMESPACE"`
	FailFast               *bool   `long:"fail-fast" description:"fail fast instead of waiting during start-up" env:"OG_EXPORTER_FAIL_FAST"`
	ListenAddress          *string `long:"listen-address" description:"prometheus web server listen address" default:":8080" env:"OG_EXPORTER_LISTEN_ADDRESS"`
	DryRun                 *bool   `long:"dry-run" description:"dry run and print raw configs"`
	ExplainOnly            *bool   `long:"explain" description:"explain server planned queries"`
	Parallel               *int    `long:"parallel" description:"Specify the parallelism. \nthe degree of parallelism is now useful query database thread "`
	DisableSettingsMetrics *bool
	TimeToString           *bool
	IsMemPprof             *bool
	Pprof                  *bool

	MetricPath               *string `long:"telemetry-path" description:"URL path under which to expose metrics." default:"/metrics" env:"METRIC_PATH"`
	MaxRequests              *int    `long:"max-requests" description:"Maximum number of parallel scrape requests. Use 0 to disable." env:"MAX_REQUESTS"`
	DisableDefaultCollectors *bool   `long:"disable-defaults" description:"Set all collectors to disabled by default." env:"MAX_REQUESTS"`
	MaxProcs                 *int    `long:"gomaxprocs" description:"The target number of CPUs Go will run on (GOMAXPROCS)." env:"GOMAXPROCS"`
	DisableExporterMetrics   *bool   `long:"disable-exporter-metrics" description:"Exclude metrics about the exporter itself (promhttp_*, process_*, go_*)."`
}

// handler wraps an unfiltered http.Handler but uses a filtered handler,
// created on the fly, if filtering is requested. Create instances with
// newHandler.
type handler struct {
	unfilteredHandler http.Handler
	// exporterMetricsRegistry is a separate registry for the metrics about
	// the exporter itself.
	exporterMetricsRegistry *prometheus.Registry
	includeExporterMetrics  bool
	maxRequests             int
	logger                  log.Logger
}

func newHandler(includeExporterMetrics bool, maxRequests int, logger log.Logger) *handler {
	h := &handler{
		exporterMetricsRegistry: prometheus.NewRegistry(),
		includeExporterMetrics:  includeExporterMetrics,
		maxRequests:             maxRequests,
		logger:                  logger,
	}
	if h.includeExporterMetrics {
		h.exporterMetricsRegistry.MustRegister(
			promcollectors.NewProcessCollector(promcollectors.ProcessCollectorOpts{}),
			promcollectors.NewGoCollector(),
		)
	}
	if innerHandler, err := h.innerHandler(); err != nil {
		panic(fmt.Sprintf("Couldn't create metrics handler: %s", err))
	} else {
		h.unfilteredHandler = innerHandler
	}
	return h
}

// ServeHTTP implements http.Handler.
func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	filters := r.URL.Query()["collect[]"]
	level.Debug(h.logger).Log("msg", "collect query:", "filters", filters)

	if len(filters) == 0 {
		// No filters, use the prepared unfiltered handler.
		h.unfilteredHandler.ServeHTTP(w, r)
		return
	}
	// To serve filtered metrics, we create a filtering handler on the fly.
	filteredHandler, err := h.innerHandler(filters...)
	if err != nil {
		level.Warn(h.logger).Log("msg", "Couldn't create filtered metrics handler:", "err", err)
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(fmt.Sprintf("Couldn't create filtered metrics handler: %s", err)))
		return
	}
	filteredHandler.ServeHTTP(w, r)
}

// innerHandler is used to create both the one unfiltered http.Handler to be
// wrapped by the outer handler and also the filtered handlers created on the
// fly. The former is accomplished by calling innerHandler without any arguments
// (in which case it will log all the collectors enabled via command-line
// flags).
func (h *handler) innerHandler(filters ...string) (http.Handler, error) {
	nc, err := collector.NewNodeCollector(h.logger, filters...)
	if err != nil {
		return nil, fmt.Errorf("couldn't create collector: %s", err)
	}

	// Only log the creation of an unfiltered handler, which should happen
	// only once upon startup.
	if len(filters) == 0 {
		level.Info(h.logger).Log("msg", "Enabled collectors")
		collectors := []string{}
		for n := range nc.Collectors {
			collectors = append(collectors, n)
		}
		sort.Strings(collectors)
		for _, c := range collectors {
			level.Info(h.logger).Log("collector", c)
		}
	}

	r := prometheus.NewRegistry()
	r.MustRegister(versioncollector.NewCollector("node_exporter"))
	if err := r.Register(nc); err != nil {
		return nil, fmt.Errorf("couldn't register node collector: %s", err)
	}

	var handler http.Handler
	if h.includeExporterMetrics {
		handler = promhttp.HandlerFor(
			prometheus.Gatherers{h.exporterMetricsRegistry, r},
			promhttp.HandlerOpts{
				ErrorLog:            stdlog.New(log.NewStdlibAdapter(level.Error(h.logger)), "", 0),
				ErrorHandling:       promhttp.ContinueOnError,
				MaxRequestsInFlight: h.maxRequests,
				Registry:            h.exporterMetricsRegistry,
			},
		)
		// Note that we have to use h.exporterMetricsRegistry here to
		// use the same promhttp metrics for all expositions.
		handler = promhttp.InstrumentMetricHandler(
			h.exporterMetricsRegistry, handler,
		)
	} else {
		handler = promhttp.HandlerFor(
			r,
			promhttp.HandlerOpts{
				ErrorLog:            stdlog.New(log.NewStdlibAdapter(level.Error(h.logger)), "", 0),
				ErrorHandling:       promhttp.ContinueOnError,
				MaxRequestsInFlight: h.maxRequests,
			},
		)
	}

	return handler, nil
}

// initArgs 初始化启动参数
func initArgs(args *Args) {
	// 增加版本信息
	kingpin.Version(version.Print("opengauss_exporter"))

	args.DbURL = kingpin.Flag("url", "openGauss database target url").
		Default("").
		Envar("OG_EXPORTER_URL").
		String()
	args.ConfigPath = kingpin.Flag("config", "path to config dir or file.").
		Default("").
		Envar("OG_EXPORTER_CONFIG").
		String()
	args.ConstLabels = kingpin.Flag("constantLabels", "A list of label=value separated by comma(,).").
		Default("").
		Envar("OG_EXPORTER_CONSTANT_LABELS").
		String()
	args.DisableCache = kingpin.Flag("disable-cache", "force not using cache").
		Default("false").
		Envar("OG_EXPORTER_DISABLE_CACHE").
		Bool()
	args.AutoDiscovery = kingpin.Flag("auto-discover-databases", "Whether to discover the databases on a server dynamically.").
		Default("false").
		Envar("OG_EXPORTER_AUTO_DISCOVER_DATABASES").
		Bool()
	args.IncludeDatabase = kingpin.Flag("include-databases", "A list of databases to add when autoDiscoverDatabases is enabled").
		Default("").
		Envar("OG_EXPORTER_INCLUDE_DATABASES").
		String()
	args.ExcludeDatabase = kingpin.Flag("exclude-databases", "A list of databases to remove when autoDiscoverDatabases is enabled").
		Default("template0,template1").
		Envar("OG_EXPORTER_EXCLUDE_DATABASES").
		String()
	args.ExporterNamespace = kingpin.Flag("namespace", "prefix of built-in metrics, (og) by default").
		Default("pg").
		Envar("OG_EXPORTER_NAMESPACE").
		String()
	args.ListenAddress = kingpin.Flag("web.listen-address", "Address to listen on for web interface and telemetry.").
		Default(":9153").
		Envar("OG_EXPORTER_WEB_LISTEN_ADDRESS").
		String()

	args.TimeToString = kingpin.Flag("time-to-string", "convert database timestamp to date string.").
		Default("false").
		Envar("OG_EXPORTER_TIME_TO_STRING").
		Bool()
	args.DryRun = kingpin.Flag("dry-run", "dry run and print default configs and user config").
		Bool()

	args.DisableSettingsMetrics = kingpin.Flag("disable-settings-metrics",
		"Do not include pg_settings metrics.").
		Default("false").
		Envar("OG_EXPORTER_DISABLE_SETTINGS_METRICS").
		Bool()

	args.ExplainOnly = kingpin.Flag("explain", "explain server planned queries").
		Bool()
	args.Parallel = kingpin.Flag("parallel", "Specify the parallelism. \nthe degree of parallelism is now useful query database thread").
		Default("5").
		Envar("OG_EXPORTER_PARALLEL").
		Int()
	args.IsMemPprof = kingpin.Flag("mem", "Turn on memory pprof When diagnosing performance issues").Default("false").Bool()
	args.Pprof = kingpin.Flag("pprof", "Turn on debug/pprof When diagnosing performance issues").Default("false").Bool()

	// Node Exporter 参数
	args.MetricPath = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").
		Default("/metrics").
		String()
	args.MaxRequests = kingpin.Flag(
		"web.max-requests",
		"Maximum number of parallel scrape requests. Use 0 to disable.",
	).Default("40").Int()
	args.DisableDefaultCollectors = kingpin.Flag(
		"collector.disable-defaults",
		"Set all collectors to disabled by default.",
	).Default("false").Bool()
	args.DisableExporterMetrics = kingpin.Flag(
		"web.disable-exporter-metrics",
		"Exclude metrics about the exporter itself (promhttp_*, process_*, go_*).",
	).Bool()
	args.MaxProcs = kingpin.Flag(
		"runtime.gomaxprocs", "The target number of CPUs Go will run on (GOMAXPROCS)",
	).Envar("GOMAXPROCS").Default("1").Int()

	//toolkitFlags = kingpinflag.AddFlags(kingpin.CommandLine, ":9100")

	//log.AddFlags(kingpin.CommandLine)
}

func main() {
	if err := config.InitConfig("./default_all.yml"); err != nil {
		slog.Error("Init Config failed", slog.Any("error", err))
		os.Exit(1)
	}

	initArgs(args)
	kingpin.Parse()

	promlogConfig := &promlog.Config{}
	flag.AddFlags(kingpin.CommandLine, promlogConfig)
	kingpin.CommandLine.UsageWriter(os.Stdout)
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()
	logger := promlog.New(promlogConfig)
	if *args.DisableDefaultCollectors {
		collector.DisableDefaultCollectors()
	}
	level.Info(logger).Log("msg", "Starting node_exporter", "version", version.Info())
	level.Info(logger).Log("msg", "Build context", "build_context", version.BuildContext())
	if user, err := user.Current(); err == nil && user.Uid == "0" {
		level.Warn(logger).Log("msg", "Node Exporter is running as root user. This exporter is designed to run as unprivileged user, root is not required.")
	}
	runtime.GOMAXPROCS(*args.MaxProcs)
	level.Debug(logger).Log("msg", "Go MAXPROCS", "procs", runtime.GOMAXPROCS(0))

	router := http.NewServeMux()
	router.Handle(*args.MetricPath, promhttp.Handler())
	// basic information
	router.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=UTF-8")
		_, _ = w.Write([]byte(`<html><head><title>PG Exporter</title></head><body><h1>PG Exporter</h1><p><a href='` + *args.MetricPath + `'>Metrics</a></p></body></html>`))
	})
	// version report
	router.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=UTF-8")
		payload := fmt.Sprintf("version %s", version.Info())
		_, _ = w.Write([]byte(payload))
	})

	if args.Pprof != nil && *args.Pprof {
		router.HandleFunc("/debug/pprof/", np.Index)
		router.HandleFunc("/debug/pprof/cmdline", np.Cmdline)
		router.HandleFunc("/debug/pprof/profile", np.Profile)
		router.HandleFunc("/debug/pprof/symbol", np.Symbol)
		router.HandleFunc("/debug/pprof/trace", np.Trace)
	}
	// reload interface
	router.HandleFunc("/reload", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=UTF-8")
		if err := Reload(); err != nil {
			w.WriteHeader(500)
			_, _ = w.Write([]byte(fmt.Sprintf("fail to reload: %s", err.Error())))
		} else {
			_, _ = w.Write([]byte(`server reloaded`))
		}
	})

	slog.Info(fmt.Sprintf("opengauss_exporter start, listen on http://%s%s", *args.ListenAddress, *args.MetricPath))

	srv := &http.Server{
		Addr:        *args.ListenAddress,
		Handler:     router,
		ReadTimeout: 5 * time.Second,
	}
	if err := newExporter(logger, nil); err != nil {
		slog.Error("Error starting newExporter", slog.Any("err", err))
		os.Exit(1)
	}
	go func() {
		// service connections
		// if err := srv.ListenAndServeTLS("server.crt", "server.key"); err != nil && err != http.ErrServerClosed {
		// 	logrus.Fatalf("listen: %s\n", err)
		// }
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("listen failed!", slog.Any("err", err))
			os.Exit(1)
		}
	}()
	closeChan := make(chan struct{}, 1)
	go func() {
		sigChan := make(chan os.Signal, 2)
		signal.Notify(sigChan, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGHUP) //nolint:staticcheck
		defer signal.Stop(sigChan)
		for {
			sig := <-sigChan
			switch sig {
			case syscall.SIGHUP:
				slog.Info(fmt.Sprintf("signal %s received, reloading", sig))
				_ = Reload()
			default:
				slog.Info(fmt.Sprintf("signal %s received, forcefully terminating", sig))
				closeChan <- struct{}{}
				return
			}
		}
	}()

	<-closeChan
	slog.Info("Shutdown Server ...")
	if err := srv.Shutdown(context.Background()); err != nil {
		slog.Error("Server Shutdown...", slog.Any("err", err))
	}

}

func Reload() error {
	ReloadLock.Lock()
	defer ReloadLock.Unlock()
	slog.Info("reload request received, launch new exporter instance")

	//create a new exporter
	//newExporter, err := newOgExporter(args)
	//// if launch new exporter failed, do nothing
	//if err != nil {
	//	log.Errorf("fail to reload exporter: %s", err.Error())
	//	return err
	//}
	//
	//log.Debugf("shutdown old exporter instance")
	//// if older one exists, close and unregister it
	//if ogExporter != nil {
	//	// DO NOT MANUALLY CLOSE OLD EXPORTER INSTANCE because the stupid implementation of sql.DB
	//	// there connection will be automatically released after 1 min
	//	prometheus.Unregister(ogExporter)
	//	ogExporter.Close()
	//}
	//prometheus.MustRegister(newExporter)
	//ogExporter = newExporter
	//log.Infof("server reloaded")
	return nil
}

func newExporter(logger log.Logger, filters []string) error {
	nc, err := collector.NewNodeCollector(logger, filters...)
	if err != nil {
		return fmt.Errorf("couldn't create collector: %s", err)
	}
	//
	//// Only log the creation of an unfiltered handler, which should happen
	//// only once upon startup.
	if len(filters) == 0 {
		slog.Info("Enabled collectors")
		collectors := []string{}
		for n := range nc.Collectors {
			collectors = append(collectors, n)
		}
		sort.Strings(collectors)
		for _, c := range collectors {
			slog.Info("collector List", slog.String("collector", c))
		}
	}
	prometheus.MustRegister(nc)
	return nil
}
