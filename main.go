package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jnemeiksis/ceph-api/api"
)

// simple json array
type jsonarray []string

// json bucket stats structure
type bucketstatsjson struct {
	Bucket    string `json:"bucket"`
    NumShards int64 `json:"num_shards"`
	ID        string `json:"id"`
	Marker    string `json:"marker"`
	Owner     string `json:"owner"`
	Ver       string `json:"ver"`
	Mtime     string `json:"mtime"`
	MaxMarker string `json:"max_marker"`
	Usage     struct {
		RgwMain struct {
			SizeKb       int64 `json:"size_kb"`
			SizeKbActual int64 `json:"size_kb_actual"`
			NumObjects   int64 `json:"num_objects"`
		} `json:"rgw.main"`
		RgwNone struct {
			SizeKb       int64 `json:"size_kb"`
			SizeKbActual int64 `json:"size_kb_actual"`
			NumObjects   int64 `json:"num_objects"`
		} `json:"rgw.none"`
		RgwMultimeta struct {
			SizeKb       int64 `json:"size_kb"`
			SizeKbActual int64 `json:"size_kb_actual"`
			NumObjects   int64 `json:"num_objects"`
		} `json:"rgw.multimeta"`
	} `json:"usage"`
	BucketQuota struct {
		Enabled    bool
		MaxSizeKb  int64 `json:"max_size_kb"`
		MaxObjects int64 `json:"max_objects"`
	} `json:"bucket_quota"`
}

// Userstats - user stats json structure
type userstatsjson struct {
	Stats struct {
		SizeKb       int64 `json:"size_kb"`
		SizeKbActual int64 `json:"size_kb_actual"`
		NumObjects   int64 `json:"num_objects"`
	} `json:"stats"`
}

// actual data needed for prometheus exporter
type bucketstatsprom struct {
	Bucket       string
	Owner        string
	NumObjects   int64
	SizeKbActual int64
	NumShards    int64
}

type bucketquotasjson struct {
	Enabled    bool  `json:"enabled"`
	MaxSizeKb  int64 `json:"max_size_kb"`
	MaxObjects int64 `json:"max_objects"`
}

type bucketquotasprom struct {
	Enabled    bool
	Owner      string
	MaxSizeKb  int64
	MaxObjects int64
}

type userquotasjson struct {
	Enabled    bool  `json:"enabled"`
	MaxSizeKb  int64 `json:"max_size_kb"`
	MaxObjects int64 `json:"max_objects"`
}

type userquotasprom struct {
	Enabled    bool
	Owner      string
	MaxSizeKb  int64
	MaxObjects int64
}

type userstatsprom struct {
	Owner        string
	NumObjects   int64
	SizeKbActual int64
}

// address/port to listen on for exporter
var addr = flag.String("listen-address", ":19128", "The address to listen on for cephrgw exporter HTTP requests.")

// bucket num objects Gauge
var (
	numObjects = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cephrgw_bucket_num_objects",
			Help: "Ceph radosgw bucket num objects from admin api",
		},
		[]string{"bucket", "owner"},
	)
)

// bucket actual size in kb Gauge
var (
	sizeKbActual = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cephrgw_bucket_size_kb",
			Help: "Ceph radosgw bucket size kb from admin api",
		},
		[]string{"bucket", "owner"},
	)
)

// user num objects Gauge
var (
	UsernumObjects = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cephrgw_user_num_objects",
			Help: "Ceph radosgw user num objects from admin api",
		},
		[]string{"owner"},
	)
)

// user actual size in kb Gauge
var (
	UsersizeKbActual = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cephrgw_user_size_kb",
			Help: "Ceph radosgw user size kb from admin api",
		},
		[]string{"owner"},
	)
)

// bucket num shards
var (
	NumShards = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cephrgw_bucket_num_shards",
			Help: "Ceph radosgw bucket num shards from admin api",
		},
		[]string{"bucket", "owner"},
	)
)

// user quota max size in kb Gauge
var (
	quotaUserMaxSizeKb = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cephrgw_quota_user_max_size_kb",
			Help: "Ceph radosgw user quota max size kb from admin api",
		},
		[]string{"owner"},
	)
)

// user quota max objects Gauge
var (
	quotaUserMaxnumObjects = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cephrgw_quota_user_max_objects",
			Help: "Ceph radosgw user quota max objects from admin api",
		},
		[]string{"owner"},
	)
)

// user quota enabled Gauge
var (
	quotaUserEnabled = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cephrgw_quota_user_enabled",
			Help: "Ceph radosgw user quota enabled from admin api",
		},
		[]string{"owner"},
	)
)

// bucket quota enabled Gauge
var (
	quotaBucketEnabled = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cephrgw_quota_bucket_enabled",
			Help: "Ceph radosgw bucket quota enabled from admin api",
		},
		[]string{"owner"},
	)
)

// bucket quota max size in kb Gauge
var (
	quotaBucketMaxSizeKb = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cephrgw_quota_bucket_max_size_kb",
			Help: "Ceph radosgw bucket quota max size kb from admin api",
		},
		[]string{"owner"},
	)
)

// bucket quota max objects Gauge
var (
	quotaBucketMaxnumObjects = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "cephrgw_quota_bucket_max_objects",
			Help: "Ceph radosgw bucket quota max objects from admin api",
		},
		[]string{"owner"},
	)
)

func boolToFloat(mybool bool) float64{
    if mybool {
        return 1
    }
    return 0
}

func init() {
	prometheus.MustRegister(numObjects)
	prometheus.MustRegister(sizeKbActual)
	prometheus.MustRegister(NumShards)
	prometheus.MustRegister(UsernumObjects)
	prometheus.MustRegister(UsersizeKbActual)
	prometheus.MustRegister(quotaUserMaxSizeKb)
	prometheus.MustRegister(quotaUserMaxnumObjects)
	prometheus.MustRegister(quotaUserEnabled)
	prometheus.MustRegister(quotaBucketEnabled)
	prometheus.MustRegister(quotaBucketMaxSizeKb)
	prometheus.MustRegister(quotaBucketMaxnumObjects)
}

func main() {
	endpoint := ""
	if len(os.Args) > 1 {
		endpoint = os.Args[1]
	} else {
		fmt.Println("Please, provide an S3 endpoint as first argument")
		log.Fatal("S3 endpoint is not provided")
		os.Exit(1)
	}

	var buckets jsonarray
	var users jsonarray
	go func() {
		for {
			// get list of users from ceph admin api
			respu := api.ListUsers(endpoint)
			json.Unmarshal([]byte(respu), &users)

			// get user quotas for each user
			uquotas := getUserQuotas(endpoint, users)

			// get bucket quotas for each user
			bquotas := getBucketQuotas(endpoint, users)

			// get buckets form ceph admin api
			respb := api.ListBucketsJSON(endpoint)
			json.Unmarshal([]byte(respb), &buckets)

			// get buckets stats
			bstats := getBucketsStats(endpoint, buckets)

			// get users stats
			ustats := getUsersStats(endpoint, users)

			// We need to reset counters before each update to clean up obsolete values
			quotaUserMaxSizeKb.Reset()
			quotaUserMaxnumObjects.Reset()
			quotaUserEnabled.Reset()
			quotaBucketEnabled.Reset()
			quotaBucketMaxSizeKb.Reset()
			quotaBucketMaxnumObjects.Reset()

			numObjects.Reset()
			sizeKbActual.Reset()
			NumShards.Reset()
			UsernumObjects.Reset()
			UsersizeKbActual.Reset()


			for i := range uquotas {
				quotaUserMaxSizeKb.WithLabelValues(uquotas[i].Owner).Set(float64(uquotas[i].MaxSizeKb))
				quotaUserMaxnumObjects.WithLabelValues(uquotas[i].Owner).Set(float64(uquotas[i].MaxObjects))
				quotaUserEnabled.WithLabelValues(uquotas[i].Owner).Set(boolToFloat(uquotas[i].Enabled))
			}
			for i := range bquotas {
				if bquotas[i].MaxSizeKb == 0 {
					quotaBucketMaxSizeKb.WithLabelValues(bquotas[i].Owner).Set(float64(-1))
				} else {
					quotaBucketMaxSizeKb.WithLabelValues(bquotas[i].Owner).Set(float64(bquotas[i].MaxSizeKb))
				}
				quotaBucketMaxnumObjects.WithLabelValues(bquotas[i].Owner).Set(float64(bquotas[i].MaxObjects))
				quotaBucketEnabled.WithLabelValues(bquotas[i].Owner).Set(boolToFloat(bquotas[i].Enabled))
			}
			for i := range bstats {
				numObjects.WithLabelValues(bstats[i].Bucket, bstats[i].Owner).Set(float64(bstats[i].NumObjects))
				sizeKbActual.WithLabelValues(bstats[i].Bucket, bstats[i].Owner).Set(float64(bstats[i].SizeKbActual))
				NumShards.WithLabelValues(bstats[i].Bucket, bstats[i].Owner).Set(float64(bstats[i].NumShards))
			}
			for i := range ustats {
				UsernumObjects.WithLabelValues(ustats[i].Owner).Set(float64(ustats[i].NumObjects))
				UsersizeKbActual.WithLabelValues(ustats[i].Owner).Set(float64(ustats[i].SizeKbActual))
			}
			time.Sleep(1 * time.Minute)
		}
	}()

	// metrics endpoint
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(*addr, nil))

}

// getBucketsStats call ceph admin api for each bucket stats and return an array
// containing info for prometheus exporter
func getBucketsStats(endpoint string, buckets jsonarray) []bucketstatsprom {
	bucketsstats := make([]bucketstatsprom, len(buckets))
	for i := range buckets {
		var bsprom bucketstatsprom
		var bstats bucketstatsjson

		// get stats for each bucket
		respbs := api.GetBucketStatsJSON(endpoint, buckets[i])
		json.Unmarshal([]byte(respbs), &bstats)
		bsprom.Bucket = buckets[i]
		bsprom.Owner = bstats.Owner
		bsprom.NumShards = bstats.NumShards
		bsprom.NumObjects = bstats.Usage.RgwMain.NumObjects
		bsprom.SizeKbActual = bstats.Usage.RgwMain.SizeKbActual
		bucketsstats[i] = bsprom
	}
	return bucketsstats
}

// getUsersStats call ceph admin api for each user stats and return an array
// containing info for prometheus exporter
func getUsersStats(endpoint string, users jsonarray) []userstatsprom {
	usersstats := make([]userstatsprom, len(users))
	for i := range users {
		var usprom userstatsprom
		var ustats userstatsjson

		// get stats for each user
		respbs := api.GetUserStatsJSON(endpoint, users[i])
		json.Unmarshal([]byte(respbs), &ustats)
		usprom.Owner = users[i]
		usprom.NumObjects = ustats.Stats.NumObjects
		usprom.SizeKbActual = ustats.Stats.SizeKbActual
		usersstats[i] = usprom
	}
	return usersstats
}

// getUserQuotas call ceph admin api for each user quota stats and return an array
// containing info for prometheus exporter
func getUserQuotas(endpoint string, users jsonarray) []userquotasprom {
	userquotas := make([]userquotasprom, len(users))
	for i := range users {
		var uqprom userquotasprom
		var uquotas userquotasjson

		// get quota for each user
		respuq := api.GetUserQuotasJSON(endpoint, users[i])
		json.Unmarshal([]byte(respuq), &uquotas)
		uqprom.Enabled = uquotas.Enabled
		uqprom.Owner = users[i]
		uqprom.MaxSizeKb = uquotas.MaxSizeKb
		uqprom.MaxObjects = uquotas.MaxObjects
		userquotas[i] = uqprom
	}
	return userquotas
}

// getBucketQuotas call ceph admin api for each user bucket quota stats and return an array
// containing info for prometheus exporter
func getBucketQuotas(endpoint string, users jsonarray) []bucketquotasprom {
	bucketquotas := make([]bucketquotasprom, len(users))
	for i := range users {
		var bqprom bucketquotasprom
		var bquotas bucketquotasjson

		// get quota for each user
		respuq := api.GetBucketQuotasJSON(endpoint, users[i])
		json.Unmarshal([]byte(respuq), &bquotas)
		bqprom.Enabled = bquotas.Enabled
		bqprom.Owner = users[i]
		bqprom.MaxSizeKb = bquotas.MaxSizeKb
		bqprom.MaxObjects = bquotas.MaxObjects
		bucketquotas[i] = bqprom
	}
	return bucketquotas
}
