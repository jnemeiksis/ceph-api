package api

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
        "github.com/aws/aws-sdk-go/aws/signer/v4"
)

// Buckets array
type Buckets []string

// Bucketstats - bucket stats json structure
type Bucketstats struct {
	Bucket    string `json:"bucket"`
	Pool      string `json:"pool"`
	IndexPool string `json:"index_pool"`
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

// ListBuckets returns buckets list array
func ListBuckets(endpoint string) Buckets {
	var b Buckets
	bjson := ListBucketsJSON(endpoint)
	json.Unmarshal([]byte(bjson), &b)
	return b
}

// GetBucketStats return bucket stats
func GetBucketStats(endpoint string, bucket string) Bucketstats {
	var bs Bucketstats
	bsjson := GetBucketStatsJSON(endpoint, bucket)
	json.Unmarshal([]byte(bsjson), &bs)
	return bs
}

// ListBucketsJSON list all the buckets in a zonegroup
func ListBucketsJSON(endpoint string) string {
	url := endpoint + "/admin/bucket"
	buckets := adminAPI(url)
	return buckets
}

// GetBucketStatsJSON return bucket stats in json
func GetBucketStatsJSON(endpoint string, bucket string) string {
	url := endpoint + "/admin/bucket?bucket=" + bucket
	bstats := adminAPI(url)
	return bstats
}

// ListUsers list all the users in a zonegroup
func ListUsers(endpoint string) string {
	url := endpoint + "/admin/metadata/user"
	buckets := adminAPI(url)
	return buckets
}

// GetUserBuckets return bucket stats
func GetUserBuckets(endpoint string, user string) string {
	url := endpoint + "/admin/bucket?uid=" + user
	ubuckets := adminAPI(url)
	return ubuckets
}

// GetUserBuckets return user quotas
func GetUserQuotasJSON(endpoint string, user string) string {
	url := endpoint + "/admin/user?quota&quota-type=user&uid=" + user
	uquotas := adminAPI(url)
	return uquotas
}

// GetUserBuckets return bucket quotas
func GetUserQuotasJSON(endpoint string, user string) string {
	url := endpoint + "/admin/user?quota&quota-type=bucket&uid=" + user
	bquotas := adminAPI(url)
	return bquotas
}

// adminApi calls specific admin api url
func adminAPI(url string) string {

	const (
		timeFormat = "20060102T150405Z"
	)

	signer := v4.NewSigner(credentials.NewEnvCredentials())

	client := &http.Client{
		Timeout: time.Second * 600,
	}

	req, _ := http.NewRequest("GET", url, nil)
	_, _ = signer.Sign(req, nil, "s3", "us-east-1", time.Now())
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal("Ceph Api response err ", err)
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)
	return string(body)
}
