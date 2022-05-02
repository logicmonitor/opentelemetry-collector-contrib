package lmexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"strconv"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/model/pdata"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

const (
	logIngestURI     = "/log/ingest"
	baseURL          = "/rest"
	hostname         = "hostname"
	hostnameProperty = "system.hostname"
	apiV3            = "3"
)

type responseBody struct {
	Message string `json:"message"`
}

type logsExporter struct {
	config *Config
	logger *zap.Logger
	client HttpClient
}

var resourceMapperMap = make(map[string]string)
var resourceMapperKey string
var metadataMap map[string]string = make(map[string]string)

// Crete new exporter.
func newLogsExporter(cfg config.Exporter, logger *zap.Logger) (*logsExporter, error) {
	oCfg := cfg.(*Config)

	newClient := NewLMHTTPClient(oCfg.APIToken, oCfg.Headers)
	if oCfg.URL != "" {
		u, err := url.Parse(oCfg.URL)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return nil, fmt.Errorf("URL must be a valid")
		}
	}

	return &logsExporter{
		config: oCfg,
		logger: logger,
		client: newClient,
	}, nil
}

func (e *logsExporter) PushLogData(ctx context.Context, lg pdata.Logs) (er error) {
	payload := ""
	resourceLogs := lg.ResourceLogs()
	for i := 0; i < resourceLogs.Len(); i++ {
		resourceLog := resourceLogs.At(i)
		libraryLogs := resourceLog.ScopeLogs()
		for j := 0; j < libraryLogs.Len(); j++ {
			libraryLog := libraryLogs.At(j)
			logs := libraryLog.LogRecords()
			for k := 0; k < logs.Len(); k++ {
				log := logs.At(k)
				// Copying resource attributes to log attributes
				resourceLog.Resource().Attributes().Range(func(k string, v pcommon.Value) bool {
					log.Attributes().Insert(k, v)
					return true
				})
				payload = e.createPayload(log.Body().StringVal(), payload, log.Attributes())
			}
		}
	}
	payload = "[\n" + payload + "]\n"

	go e.export(payload)
	return nil
}

// Send data to endpoint
func (e *logsExporter) export(payload string) ([]byte, int, error) {
	timeout := 5 * time.Second
	bytesPayload := []byte(payload)
	resp, err := e.client.MakeRequest(apiV3, http.MethodPost, baseURL, logIngestURI, e.config.URL, timeout, bytes.NewBuffer(bytesPayload), nil)
	if err != nil {
		e.logger.Error("error caught in http request of logs exporter", zap.Error(err))
		return nil, 0, err
	}

	var respBody responseBody
	if err := json.Unmarshal(resp.Body, &respBody); err != nil {
		e.logger.Error("error unmarshalling response", zap.Error(err))
		return resp.Body, resp.StatusCode, err
	}

	if !(resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusAccepted) {
		e.logger.Error("error caught while sending logs with response ", zap.Error(errors.New(respBody.Message)))
		return resp.Body, resp.StatusCode, err
	}
	return nil, 0, nil
}

//Create payload according to log-ingest standards
func (e *logsExporter) createPayload(msg string, payload string, attributesMap pcommon.Map) string {
	metadataMap = make(map[string]string)
	metadataString := ""

	msg = strconv.Quote(msg)
	msg = msg[1 : len(msg)-1]

	metadataMap["_lm.logsource_type"] = "logfile"
	attributesMap.Range(rangeOverAttributesMap)
	if len(metadataMap) != 0 {
		metadataByte, err := json.Marshal(metadataMap)
		if err != nil {
			fmt.Printf("error while unmarshalling metadataMap in createPayload(): %s", err)
			return ""
		}
		metadataString = string(metadataByte)
		metadataString = metadataString[1 : len(metadataString)-1]
		metadataString = ",\n" + metadataString
	}
	tempPayload := `{
			"msg": "` + msg + `",
			"_lm.resourceId": {
				"` + resourceMapperKey + `": "` + resourceMapperMap[resourceMapperKey] + `"
			}` +
		metadataString + "\n" +
		`}`

	if payload != "" {
		payload = payload + ",\n" + tempPayload
	} else {
		payload = tempPayload
	}
	return payload
}

//rangeOverAttributesMap goes over all the attributes attached with the log line
func rangeOverAttributesMap(key string, value pcommon.Value) bool {

	if key == hostname {
		resourceMapperKey = hostnameProperty
		resourceMapperMap[resourceMapperKey] = value.StringVal()
	}
	metadataMap[key] = value.StringVal()
	return true
}
