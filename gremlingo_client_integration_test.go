//go:build integration_tests

package gremcos_test

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ameena3/gremcos" // For DialGremlinGo, WithGremlinGoAuth etc.
	"github.com/ameena3/gremcos/interfaces"
	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
)

const (
	// testGremlinServerURL should match the service in infra/docker-compose.yml
	// The default Gremlin Server port is 8182.
	testGremlinServerURL = "ws://localhost:8182/gremlin"
	// Default traversal source for most TinkerPop graphs
	testTraversalSource = "g"
)

var testLogger zerolog.Logger

// simpleGremlinGoLogger is a basic implementation of gremlingo.Logger for testing.
type simpleGremlinGoLogger struct {
	zl zerolog.Logger
}

func (s *simpleGremlinGoLogger) verbosityToString(verbosity gremlingo.LogVerbosity) string {
	switch verbosity {
	case gremlingo.Debug:
		return "DEBUG"
	case gremlingo.Info:
		return "INFO"
	case gremlingo.Warning:
		return "WARNING"
	case gremlingo.Error:
		return "ERROR"
	// gremlingo.Off (if it exists and is used for logging calls, which is unlikely)
	default:
		return fmt.Sprintf("LEVEL(%d)", verbosity)
	}
}

func (s *simpleGremlinGoLogger) Log(verbosity gremlingo.LogVerbosity, v ...interface{}) {
	s.zl.Debug().Msgf("GREMLINGO [%s]: %s", s.verbosityToString(verbosity), fmt.Sprint(v...))
}

func (s *simpleGremlinGoLogger) Logf(verbosity gremlingo.LogVerbosity, format string, v ...interface{}) {
	s.zl.Debug().Msgf("GREMLINGO [%s]: %s", s.verbosityToString(verbosity), fmt.Sprintf(format, v...))
}


func init() {
	testLogger = zerolog.New(os.Stdout).Output(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: zerolog.TimeFieldFormat}).With().Timestamp().Logger().Level(zerolog.DebugLevel)
}

// getIntegrationGremlinGoClient provides a connected client for integration tests.
func getIntegrationGremlinGoClient(t *testing.T) interfaces.QueryExecutor {
	t.Helper()

	var clientOptions []gremcos.GremlinGoClientOption
	clientOptions = append(clientOptions, gremcos.WithGremlinGoTraversalSource(testTraversalSource))
	clientOptions = append(clientOptions, gremcos.WithGremlinGoDriverLogger(&simpleGremlinGoLogger{zl: testLogger.With().Str("component", "gremlin-go-driver").Logger()}))

	client, err := gremcos.DialGremlinGo(testGremlinServerURL, clientOptions...)
	require.NoError(t, err, "DialGremlinGo should not return an error for basic client setup")
	require.NotNil(t, client, "Client should not be nil after DialGremlinGo")

	pingErr := client.Ping()
	require.NoError(t, pingErr, "Ping failed immediately after connection for integration test client")

	return client
}

// TestGremlinGoClient_Connection verifies basic connection, ping, and close functionality.
func TestGremlinGoClient_Connection(t *testing.T) {
	client := getIntegrationGremlinGoClient(t)

	assert.True(t, client.IsConnected(), "Client should be connected after successful dial and ping")

	err := client.Ping()
	assert.NoError(t, err, "Client.Ping() should not return an error for an active connection")

	err = client.Close()
	assert.NoError(t, err, "Client.Close() should not return an error")

	assert.False(t, client.IsConnected(), "Client should be marked as not connected after Close()")

	err = client.Ping()
	assert.Error(t, err, "Client.Ping() should return an error for a closed connection")
}

// TestGremlinGoClient_CRUD verifies Create, Read, Update, Delete operations.
func TestGremlinGoClient_CRUD(t *testing.T) {
	client := getIntegrationGremlinGoClient(t)
	defer func() {
		_, errCl := client.Execute("g.V().drop()")
		assert.NoError(t, errCl, "Cleanup: Failed to drop all vertices")
		errCl = client.Close()
		assert.NoError(t, errCl, "Failed to close client during defer")
	}()

	_, err := client.Execute("g.V().drop()")
	require.NoError(t, err, "Initial cleanup: Failed to drop all vertices")

	addVertexQuery := fmt.Sprintf("g.addV('testNode').property('id', 'node1').property('name', 'testName').property('value', 100)")
	responses, err := client.Execute(addVertexQuery)
	require.NoError(t, err, "CRUD Create: Failed to add vertex")
	require.Len(t, responses, 1, "CRUD Create: Expected 1 response item for addV")

	readQuery := "g.V().hasLabel('testNode').has('id', 'node1').valueMap(true)"
	responses, err = client.Execute(readQuery)
	require.NoError(t, err, "CRUD Read: Failed to read vertex")
	require.Len(t, responses, 1, "CRUD Read: Expected 1 vertex back")

	var resultData []map[string]interface{}
	err = json.Unmarshal(responses[0].Result.Data, &resultData)
	require.NoError(t, err, "CRUD Read: Failed to unmarshal result data")
	require.Len(t, resultData, 1, "CRUD Read: Expected data for 1 vertex in valueMap")

	vertexProperties := resultData[0]
	assert.Equal(t, "node1", vertexProperties["id"].([]interface{})[0], "CRUD Read: 'id' property mismatch")
	assert.Equal(t, "testName", vertexProperties["name"].([]interface{})[0], "CRUD Read: 'name' property mismatch")
	assert.Equal(t, float64(100), vertexProperties["value"].([]interface{})[0], "CRUD Read: 'value' property mismatch")

	updateQuery := "g.V().hasLabel('testNode').has('id', 'node1').property('name', 'updatedName')"
	responses, err = client.Execute(updateQuery)
	require.NoError(t, err, "CRUD Update: Failed to update vertex")
	require.True(t, len(responses) >= 1, "CRUD Update: Expected at least 1 response for update")

	responses, err = client.Execute(readQuery)
	require.NoError(t, err, "CRUD Update: Failed to re-read vertex after update")
	require.Len(t, responses, 1, "CRUD Update: Expected 1 vertex after update")
	err = json.Unmarshal(responses[0].Result.Data, &resultData)
	require.NoError(t, err, "CRUD Update: Failed to unmarshal updated result data")
	require.Len(t, resultData, 1)
	updatedVertexProperties := resultData[0]
	assert.Equal(t, "updatedName", updatedVertexProperties["name"].([]interface{})[0], "CRUD Update: 'name' was not updated")

	deleteQuery := "g.V().hasLabel('testNode').has('id', 'node1').drop()"
	responses, err = client.Execute(deleteQuery)
	require.NoError(t, err, "CRUD Delete: Failed to drop vertex")

	countQuery := "g.V().hasLabel('testNode').has('id', 'node1').count()"
	responses, err = client.Execute(countQuery)
	require.NoError(t, err, "CRUD Delete: Failed to count vertex after drop")
	require.Len(t, responses, 1, "CRUD Delete: Expected 1 response for count")
	var countResult []float64
	err = json.Unmarshal(responses[0].Result.Data, &countResult)
	require.NoError(t, err, "CRUD Delete: Failed to unmarshal count result")
	require.Len(t, countResult, 1, "Count result should have one element")
	assert.Equal(t, float64(0), countResult[0], "CRUD Delete: Vertex count should be 0 after drop")

	bindings := map[string]interface{}{"nodeId": "node2", "nodeName": "nameFromBinding", "label": "testNodeWithBinding"}
	addWithBindingsQuery := "g.addV(label).property('id', nodeId).property('name', nodeName)"
	responses, err = client.ExecuteWithBindings(addWithBindingsQuery, bindings, nil)
	require.NoError(t, err, "CRUD Bindings: Failed to add vertex with bindings")
	require.Len(t, responses, 1, "CRUD Bindings: Expected 1 response for addV with bindings")

	readBoundQuery := "g.V().hasLabel('testNodeWithBinding').has('id', 'node2').valueMap(true)"
	responses, err = client.Execute(readBoundQuery)
	require.NoError(t, err, "CRUD Bindings: Failed to read bound vertex")
	require.Len(t, responses, 1, "CRUD Bindings: Expected 1 bound vertex back")
	err = json.Unmarshal(responses[0].Result.Data, &resultData)
	require.NoError(t, err, "CRUD Bindings: Failed to unmarshal bound result data")
	require.Len(t, resultData, 1)
	boundVertexProperties := resultData[0]
	assert.Equal(t, "node2", boundVertexProperties["id"].([]interface{})[0])
	assert.Equal(t, "nameFromBinding", boundVertexProperties["name"].([]interface{})[0])
}

// TestGremlinGoClient_QueryError checks error handling for invalid queries.
func TestGremlinGoClient_QueryError(t *testing.T) {
	client := getIntegrationGremlinGoClient(t)
	defer func() {
		err := client.Close()
		assert.NoError(t, err, "Failed to close client")
	}()

	malformedQuery := "g.V().syntaxError()"
	_, err := client.Execute(malformedQuery)
	assert.Error(t, err, "Execute should return an error for a malformed query")

	lastErr := client.LastError()
	assert.Error(t, lastErr, "LastError should reflect the error from the failed query")
	if lastErr != nil {
		testLogger.Debug().Err(lastErr).Msg("Received error for malformed query (expected)")
		assert.True(t, strings.Contains(strings.ToLower(lastErr.Error()), "scriptevaluationerror") || strings.Contains(strings.ToLower(lastErr.Error()), "error"), "Error message should indicate a script or evaluation error")
	}
}

// TestGremlinGoClient_ExecuteAsync tests asynchronous query execution.
func TestGremlinGoClient_ExecuteAsync(t *testing.T) {
	client := getIntegrationGremlinGoClient(t)
	defer func() {
		_, _ = client.Execute("g.V().hasLabel('asyncNode').drop()")
		_ = client.Close()
	}()

	_, err := client.Execute("g.V().hasLabel('asyncNode').drop()")
	require.NoError(t, err)

	_, err = client.Execute("g.addV('asyncNode').property('id','async1').property('name', 'asyncName1'); g.addV('asyncNode').property('id','async2').property('name', 'asyncName2')")
	require.NoError(t, err, "Async Test: Failed to add initial data")

	responseChannel := make(chan interfaces.AsyncResponse, 5)
	query := "g.V().hasLabel('asyncNode').valueMap(true)"

	err = client.ExecuteAsync(query, responseChannel)
	require.NoError(t, err, "Async Test: ExecuteAsync initial call should not return error")

	var receivedResponses []interfaces.Response
	var receivedErrorMessages []string
	timeout := time.After(10 * time.Second)

	doneReceiving := false
	for !doneReceiving {
		select {
		case asyncResp, ok := <-responseChannel:
			if !ok {
				doneReceiving = true
				break
			}
			if asyncResp.ErrorMessage != "" {
				receivedErrorMessages = append(receivedErrorMessages, asyncResp.ErrorMessage)
				testLogger.Error().Str("asyncErrorMessage", asyncResp.ErrorMessage).Msg("Async Test: Received error on channel")
			} else {
				receivedResponses = append(receivedResponses, asyncResp.Response)
				testLogger.Debug().Interface("asyncResponse", asyncResp.Response).Msg("Async Test: Received response")
			}
		case <-timeout:
			doneReceiving = true
			t.Fatal("Async Test: Timed out waiting for responses")
		}
	}

	assert.Empty(t, receivedErrorMessages, "Async Test: Should not receive errors on the channel for a valid query")
	require.Len(t, receivedResponses, 2, "Async Test: Expected 2 responses for async query")

	var foundAsync1, foundAsync2 bool
	for _, resp := range receivedResponses {
		var resultDataList []map[string]interface{}
		err := json.Unmarshal(resp.Result.Data, &resultDataList)
		require.NoError(t, err, "Async Test: Failed to unmarshal async result data")

		for _, itemMap := range resultDataList {
			idVal, okId := itemMap["id"]
			nameVal, okName := itemMap["name"]
			if okId && okName {
				id := idVal.([]interface{})[0].(string)
				name := nameVal.([]interface{})[0].(string)
				if id == "async1" && name == "asyncName1" {
					foundAsync1 = true
				}
				if id == "async2" && name == "asyncName2" {
					foundAsync2 = true
				}
			}
		}
	}
	assert.True(t, foundAsync1, "Async Test: Did not find asyncNode with id 'async1'")
	assert.True(t, foundAsync2, "Async Test: Did not find asyncNode with id 'async2'")
}
