// Copyright 2019 - 2019 Databricks

package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	retryablehttp "github.com/hashicorp/go-retryablehttp"
)

type DatabricksSession struct {
	client  retryablehttp.Client
	uri     url.URL
	params  url.Values
	headers http.Header
}

func NewDatabricksSession(client *http.Client, host string, port uint16) (*DatabricksSession, error) {
	session := DatabricksSession{
		client: retryablehttp.Client{
			HTTPClient:   client,
			Backoff:      retryablehttp.LinearJitterBackoff,
			RetryWaitMin: 800 * time.Millisecond,
			RetryWaitMax: 1200 * time.Millisecond,
			// we want to wait up to ~5 minutes total,
			// (because data daemon might go down up to
			// that long) across all retries because
			// Backoff is LinearJitterBackoff, we retry
			// approximately once a second, so to get to
			// 300s retry we need to solve for n: (1+n)n/2
			// = 300, n = 24
			RetryMax:   24,
			CheckRetry: retryablehttp.DefaultRetryPolicy,
			RequestLogHook: func(_ retryablehttp.Logger, r *http.Request, nRetry int) {
				if nRetry != 0 {
					s3Log.Infof("databricks api retry #%v: %v", nRetry, r.URL)
				}
			},
			ResponseLogHook: func(_ retryablehttp.Logger, r *http.Response) {
				if r.StatusCode >= 500 {
					s3Log.Errorf("databricks api %v: %v", r.Request.URL, r.Status)
				}
			},
		},
		uri: url.URL{
			Scheme: "http",
			Host:   fmt.Sprintf("%v:%v", host, port),
			Path:   "/",
		},
		params: url.Values{},
		headers: http.Header{
			"auth":     []string{"{}"},
			"authType": []string{"com.databricks.backend.daemon.data.common.DbfsAuth"},
		},
	}

	res := SessionCreateResponse{}
	err := session.Call("SessionCreate", &res, nil)
	if err != nil {
		return nil, err
	}

	session.headers.Add("sessionid", fmt.Sprintf("%v", res.Id))
	return &session, nil
}

// return (bucket, prefix)
// prefix doesn't have leading or trailing /
func (m *GetMountsV2Response) Bucket() (bucket string, prefix string, err error) {
	// SourceString looks like s3a://bucket/path
	var uri *url.URL

	uri, err = url.ParseRequestURI(m.SourceString)
	if err != nil {
		return
	}

	bucket = uri.Hostname()
	prefix = strings.Trim(uri.Path, "/")

	return
}

func (s *DatabricksSession) GetMountsV2() (res []GetMountsV2Response, err error) {
	res = []GetMountsV2Response{}
	err = s.Call("GetMountsV2", &res, nil)
	return
}

func (s *DatabricksSession) FindMount(mountPoint string) (*GetMountsV2Response, error) {
	mounts, err := s.GetMountsV2()
	if err != nil {
		return nil, err
	}

	for _, m := range mounts {
		if m.MountPointString == mountPoint {
			return &m, nil
		}
	}

	return nil, err
}

func (s *DatabricksSession) configureDatabricksMount(config *DatabricksConf, mountPoint string, prefix string,
	flags *FlagStorage, awsConfig *aws.Config) (bucketSpec string, err error) {

	if config.Endpoint != "" {
		flags.Endpoint = "https://" + config.Endpoint
	}

	var m *GetMountsV2Response
	m, err = s.FindMount(mountPoint)
	if err != nil {
		return
	}

	var bucket, bucketPrefix string
	bucket, bucketPrefix, err = m.Bucket()
	if err != nil {
		return
	}
	bucketSpec = fmt.Sprintf("%v:%v/%v", bucket, bucketPrefix, prefix)

	if m.Configurations.CredentialsType == "SessionToken" || config.SessionTokenAllowed {
		awsConfig.Credentials = credentials.NewCredentials(NewDatabricksCredentialsProvider(s, mountPoint))
	} else {
		// this could be empty which means default, so
		// fallback to default aws default provider chain
	}
	flags.UseSSE = true
	if m.Configurations.Endpoint != "" {
		flags.Endpoint = "https://" + m.Configurations.Endpoint
	} else if m.Configurations.FallbackEndpoint != "" {
		flags.Endpoint = "https://" + m.Configurations.FallbackEndpoint
	}

	return
}

func (s *DatabricksSession) Call(api string, res interface{}, arg interface{}) error {
	// ugh 2019 and go still can't copy a map easily
	params := url.Values{}
	for k, v := range s.params {
		for _, p := range v {
			params.Add(k, p)
		}
	}

	typeParam := fmt.Sprintf("\"com.databricks.backend.daemon.data.common.DataMessages$%v\"", api)
	params.Add("type", typeParam)

	uri := s.uri
	uri.RawQuery = params.Encode()

	s3Log.Debugf("databricks api: %v", api)

	var body []byte
	var err error

	if arg != nil {
		body, err = json.Marshal(arg)
		if err != nil {
			return err
		}
	} else {
		body = []byte("{}")
	}

	req, err := retryablehttp.NewRequest("GET", uri.String(), bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header = s.headers

	resp, err := s.client.Do(req)
	if err != nil {
		s3Log.Errorf("databricks api %v error: %v", api, err)
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		err = fmt.Errorf("databricks api %v error: %v", api, resp.StatusCode)
		body, bodyErr := ioutil.ReadAll(resp.Body)
		if bodyErr != nil {
			return bodyErr
		}

		s3Log.Errorf("%v %v", err, string(body))
		return err
	}

	decoder := json.NewDecoder(resp.Body)
	err = decoder.Decode(res)
	if err != nil {
		log.Errorf("api %v decode error: %v", api, err)
	}

	s3Log.Debug(res)

	return err
}

type DatabricksCredentialsProvider struct {
	session    *DatabricksSession
	mountPoint string
	expireAt   time.Time
}

// hide the secrets from output
func (c *GetSessionCredentialsResponse) String() string {
	return fmt.Sprintf("GetSessionCredentialsResponse expiration: %v", time.Unix(c.Expiration/1000, c.Expiration%1000*1000*1000))
}

func NewDatabricksCredentialsProvider(session *DatabricksSession, mountPoint string) *DatabricksCredentialsProvider {
	return &DatabricksCredentialsProvider{
		session:    session,
		mountPoint: mountPoint,
	}
}

func (p *DatabricksCredentialsProvider) Retrieve() (cred credentials.Value, err error) {
	res := GetSessionCredentialsResponse{}
	err = p.session.Call("GetSessionCredentials", &res, map[string]string{
		"mountPoint": p.mountPoint,
	})
	if err != nil {
		return
	}

	cred.AccessKeyID = res.Key
	cred.SecretAccessKey = res.Secret
	cred.SessionToken = res.Token
	// expire the credentials 15s early so it's ok even if our clock is slightly slower
	p.expireAt = time.Unix(res.Expiration/1000, res.Expiration%1000*1000*1000).Add(-5 * time.Minute)
	s3Log.Infof("token refreshed, next expire at: %v", p.expireAt.String())

	return
}

func (p *DatabricksCredentialsProvider) IsExpired() bool {
	return p.expireAt.Before(time.Now())
}

func ConfigureDatabricksMount(bucketSpec *string,
	flags *FlagStorage, awsConfig *aws.Config) (err error) {

	config, err := NewDatabricksConf()
	if err != nil {
		return
	}

	err = config.Load()
	if err != nil {
		return
	}

	// ensure that we don't use a proxy to talk to the data daemon
	transport := http.DefaultTransport
	if t, ok := transport.(*http.Transport); ok {
		t.Proxy = nil
	}

	var session *DatabricksSession
	session, err = NewDatabricksSession(&http.Client{Transport: transport}, config.DaemonHost, DATABRICKS_DATA_DAEMON_CONTROL_PORT)
	if err != nil {
		err = fmt.Errorf("databricks session: %v", err)
		return
	}

	mountAndPrefix := strings.SplitN(*bucketSpec, ":", 2)
	if len(mountAndPrefix) != 2 {
		err = fmt.Errorf("unexpected bucket spec: %v", bucketSpec)
		return
	}

	mountPoint := mountAndPrefix[0]
	// the prefix is in addition to the storage prefix for the mount point
	prefix := strings.Trim(mountAndPrefix[1], "/")

	*bucketSpec, err = session.configureDatabricksMount(config, mountPoint, prefix, flags, awsConfig)
	log.Infof("using detected databricks mountpoint: %v", *bucketSpec)
	return
}
