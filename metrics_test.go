package gremcos

import (
	"testing"

	mock_metrics "github.com/ameena3/gremcos/test/mocks/metrics"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

type MetricsMocks struct {
	statusCodeTotal                  *mock_metrics.MockCounterVec
	retryAfterMS                     *mock_metrics.MockHistogram
	requestChargeTotal               *mock_metrics.MockCounter
	requestChargePerQuery            *mock_metrics.MockGauge
	requestChargePerQueryResponseAvg *mock_metrics.MockGauge
	serverTimePerQueryMS             *mock_metrics.MockGauge
	serverTimePerQueryResponseAvgMS  *mock_metrics.MockGauge
	connectivityErrorsTotal          *mock_metrics.MockCounter
	connectionUsageTotal             *mock_metrics.MockCounterVec
	requestErrorsTotal               *mock_metrics.MockCounter
	requestRetiesTotal               *mock_metrics.MockCounter
	requestRetryTimeoutsTotal        *mock_metrics.MockCounter
}

// NewMockedMetrics creates and returns mocked metrics that can be used
// for unit-testing.
// Example:
//
//	mockCtrl := gomock.NewController(t)
//	defer mockCtrl.Finish()
//	metrics, mocks := NewMockedMetrics(mockCtrl)
//	mocks.scaleCounter.EXPECT().Set(10)
//
// use metrics...
func NewMockedMetrics(mockCtrl *gomock.Controller) (*Metrics, *MetricsMocks) {
	mStatusCodeTotal := mock_metrics.NewMockCounterVec(mockCtrl)
	mRetryAfterMS := mock_metrics.NewMockHistogram(mockCtrl)
	mRequestChargeTotal := mock_metrics.NewMockCounter(mockCtrl)
	mRequestChargePerQuery := mock_metrics.NewMockGauge(mockCtrl)
	mRequestChargePerQueryResponseAvg := mock_metrics.NewMockGauge(mockCtrl)
	mServerTimePerQueryMS := mock_metrics.NewMockGauge(mockCtrl)
	mServerTimePerQueryResponseAvgMS := mock_metrics.NewMockGauge(mockCtrl)
	mConnectivityErrorsTotal := mock_metrics.NewMockCounter(mockCtrl)
	mConnectionUsageTotal := mock_metrics.NewMockCounterVec(mockCtrl)
	mRequestErrorsTotal := mock_metrics.NewMockCounter(mockCtrl)
	mRequestRetiesTotal := mock_metrics.NewMockCounter(mockCtrl)
	mRequestRetryTimeoutsTotal := mock_metrics.NewMockCounter(mockCtrl)

	metrics := &Metrics{
		statusCodeTotal:                  mStatusCodeTotal,
		retryAfterMS:                     mRetryAfterMS,
		requestChargeTotal:               mRequestChargeTotal,
		requestChargePerQuery:            mRequestChargePerQuery,
		requestChargePerQueryResponseAvg: mRequestChargePerQueryResponseAvg,
		serverTimePerQueryMS:             mServerTimePerQueryMS,
		serverTimePerQueryResponseAvgMS:  mServerTimePerQueryResponseAvgMS,
		connectivityErrorsTotal:          mConnectivityErrorsTotal,
		connectionUsageTotal:             mConnectionUsageTotal,
		requestErrorsTotal:               mRequestErrorsTotal,
		requestRetiesTotal:               mRequestRetiesTotal,
		requestRetryTimeoutsTotal:        mRequestRetryTimeoutsTotal,
	}

	mocks := &MetricsMocks{
		statusCodeTotal:                  mStatusCodeTotal,
		retryAfterMS:                     mRetryAfterMS,
		requestChargeTotal:               mRequestChargeTotal,
		requestChargePerQuery:            mRequestChargePerQuery,
		requestChargePerQueryResponseAvg: mRequestChargePerQueryResponseAvg,
		serverTimePerQueryMS:             mServerTimePerQueryMS,
		serverTimePerQueryResponseAvgMS:  mServerTimePerQueryResponseAvgMS,
		connectivityErrorsTotal:          mConnectivityErrorsTotal,
		connectionUsageTotal:             mConnectionUsageTotal,
		requestErrorsTotal:               mRequestErrorsTotal,
		requestRetiesTotal:               mRequestRetiesTotal,
		requestRetryTimeoutsTotal:        mRequestRetryTimeoutsTotal,
	}

	return metrics, mocks
}

func Test_NewMetrics(t *testing.T) {
	metrics := NewMetrics("gremcos")
	assert.NotNil(t, metrics.statusCodeTotal)
	assert.NotNil(t, metrics.retryAfterMS)
	assert.NotNil(t, metrics.requestChargeTotal)
	assert.NotNil(t, metrics.requestChargePerQuery)
	assert.NotNil(t, metrics.requestChargePerQueryResponseAvg)
	assert.NotNil(t, metrics.serverTimePerQueryMS)
	assert.NotNil(t, metrics.serverTimePerQueryResponseAvgMS)
	assert.NotNil(t, metrics.connectivityErrorsTotal)
	assert.NotNil(t, metrics.connectionUsageTotal)
	assert.NotNil(t, metrics.requestErrorsTotal)
	assert.NotNil(t, metrics.requestRetiesTotal)
	assert.NotNil(t, metrics.requestRetryTimeoutsTotal)
}
