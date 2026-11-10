package metrics

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

// WatchPool exports pool's connection statistics, read from pgx at each
// scrape. They answer "is the database pool a bottleneck?": how many
// connections are in use, and how often and how long requests waited for one.
// A nil Metrics (metrics off) makes it a no-op.
func (m *Metrics) WatchPool(pool *pgxpool.Pool) {
	if m == nil || pool == nil {
		return
	}
	m.reg.MustRegister(&poolCollector{pool: pool})
}

var (
	poolMaxConns = prometheus.NewDesc("idem_db_pool_max_conns",
		"Maximum connections this process's pool may open.", nil, nil)
	poolTotalConns = prometheus.NewDesc("idem_db_pool_total_conns",
		"Connections currently open (idle, in use, or being established).", nil, nil)
	poolAcquiredConns = prometheus.NewDesc("idem_db_pool_acquired_conns",
		"Connections currently in use.", nil, nil)
	poolIdleConns = prometheus.NewDesc("idem_db_pool_idle_conns",
		"Connections open but idle.", nil, nil)
	poolAcquires = prometheus.NewDesc("idem_db_pool_acquires_total",
		"Successful connection acquisitions.", nil, nil)
	poolEmptyAcquires = prometheus.NewDesc("idem_db_pool_empty_acquires_total",
		"Acquisitions that had to wait because no connection was free.", nil, nil)
	poolAcquireWait = prometheus.NewDesc("idem_db_pool_acquire_wait_seconds_total",
		"Total time spent waiting for a free connection.", nil, nil)
	poolCanceledAcquires = prometheus.NewDesc("idem_db_pool_canceled_acquires_total",
		"Acquisitions abandoned because their context ended while waiting.", nil, nil)
)

type poolCollector struct {
	pool *pgxpool.Pool
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{poolMaxConns, poolTotalConns, poolAcquiredConns, poolIdleConns,
		poolAcquires, poolEmptyAcquires, poolAcquireWait, poolCanceledAcquires} {
		ch <- d
	}
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.pool.Stat()
	g := func(d *prometheus.Desc, v float64) { ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v) }
	n := func(d *prometheus.Desc, v float64) {
		ch <- prometheus.MustNewConstMetric(d, prometheus.CounterValue, v)
	}
	g(poolMaxConns, float64(s.MaxConns()))
	g(poolTotalConns, float64(s.TotalConns()))
	g(poolAcquiredConns, float64(s.AcquiredConns()))
	g(poolIdleConns, float64(s.IdleConns()))
	n(poolAcquires, float64(s.AcquireCount()))
	n(poolEmptyAcquires, float64(s.EmptyAcquireCount()))
	n(poolAcquireWait, s.EmptyAcquireWaitTime().Seconds())
	n(poolCanceledAcquires, float64(s.CanceledAcquireCount()))
}
