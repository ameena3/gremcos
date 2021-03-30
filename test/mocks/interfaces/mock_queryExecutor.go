// Code generated by MockGen. DO NOT EDIT.
// Source: interfaces/queryExecutor.go

// Package mock_interfaces is a generated GoMock package.
package mock_interfaces

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	interfaces "github.com/supplyon/gremcos/interfaces"
)

// MockQueryExecutor is a mock of QueryExecutor interface.
type MockQueryExecutor struct {
	ctrl     *gomock.Controller
	recorder *MockQueryExecutorMockRecorder
}

// MockQueryExecutorMockRecorder is the mock recorder for MockQueryExecutor.
type MockQueryExecutorMockRecorder struct {
	mock *MockQueryExecutor
}

// NewMockQueryExecutor creates a new mock instance.
func NewMockQueryExecutor(ctrl *gomock.Controller) *MockQueryExecutor {
	mock := &MockQueryExecutor{ctrl: ctrl}
	mock.recorder = &MockQueryExecutorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockQueryExecutor) EXPECT() *MockQueryExecutorMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockQueryExecutor) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockQueryExecutorMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockQueryExecutor)(nil).Close))
}

// Execute mocks base method.
func (m *MockQueryExecutor) Execute(query string) ([]interfaces.Response, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Execute", query)
	ret0, _ := ret[0].([]interfaces.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Execute indicates an expected call of Execute.
func (mr *MockQueryExecutorMockRecorder) Execute(query interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Execute", reflect.TypeOf((*MockQueryExecutor)(nil).Execute), query)
}

// ExecuteAsync mocks base method.
func (m *MockQueryExecutor) ExecuteAsync(query string, responseChannel chan interfaces.AsyncResponse) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExecuteAsync", query, responseChannel)
	ret0, _ := ret[0].(error)
	return ret0
}

// ExecuteAsync indicates an expected call of ExecuteAsync.
func (mr *MockQueryExecutorMockRecorder) ExecuteAsync(query, responseChannel interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteAsync", reflect.TypeOf((*MockQueryExecutor)(nil).ExecuteAsync), query, responseChannel)
}

// ExecuteFile mocks base method.
func (m *MockQueryExecutor) ExecuteFile(path string) ([]interfaces.Response, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExecuteFile", path)
	ret0, _ := ret[0].([]interfaces.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExecuteFile indicates an expected call of ExecuteFile.
func (mr *MockQueryExecutorMockRecorder) ExecuteFile(path interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteFile", reflect.TypeOf((*MockQueryExecutor)(nil).ExecuteFile), path)
}

// ExecuteFileWithBindings mocks base method.
func (m *MockQueryExecutor) ExecuteFileWithBindings(path string, bindings, rebindings map[string]string) ([]interfaces.Response, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExecuteFileWithBindings", path, bindings, rebindings)
	ret0, _ := ret[0].([]interfaces.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExecuteFileWithBindings indicates an expected call of ExecuteFileWithBindings.
func (mr *MockQueryExecutorMockRecorder) ExecuteFileWithBindings(path, bindings, rebindings interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteFileWithBindings", reflect.TypeOf((*MockQueryExecutor)(nil).ExecuteFileWithBindings), path, bindings, rebindings)
}

// ExecuteWithBindings mocks base method.
func (m *MockQueryExecutor) ExecuteWithBindings(query string, bindings, rebindings map[string]string) ([]interfaces.Response, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ExecuteWithBindings", query, bindings, rebindings)
	ret0, _ := ret[0].([]interfaces.Response)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ExecuteWithBindings indicates an expected call of ExecuteWithBindings.
func (mr *MockQueryExecutorMockRecorder) ExecuteWithBindings(query, bindings, rebindings interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ExecuteWithBindings", reflect.TypeOf((*MockQueryExecutor)(nil).ExecuteWithBindings), query, bindings, rebindings)
}

// IsConnected mocks base method.
func (m *MockQueryExecutor) IsConnected() bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsConnected")
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsConnected indicates an expected call of IsConnected.
func (mr *MockQueryExecutorMockRecorder) IsConnected() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsConnected", reflect.TypeOf((*MockQueryExecutor)(nil).IsConnected))
}

// LastError mocks base method.
func (m *MockQueryExecutor) LastError() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LastError")
	ret0, _ := ret[0].(error)
	return ret0
}

// LastError indicates an expected call of LastError.
func (mr *MockQueryExecutorMockRecorder) LastError() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LastError", reflect.TypeOf((*MockQueryExecutor)(nil).LastError))
}

// Ping mocks base method.
func (m *MockQueryExecutor) Ping() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Ping")
	ret0, _ := ret[0].(error)
	return ret0
}

// Ping indicates an expected call of Ping.
func (mr *MockQueryExecutorMockRecorder) Ping() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ping", reflect.TypeOf((*MockQueryExecutor)(nil).Ping))
}
