package gremcos

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sync"

	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"github.com/ameena3/gremcos/interfaces"
)

// GremlinGoClient implements the interfaces.QueryExecutor for the gremlin-go driver.
type GremlinGoClient struct {
	url             string
	gremlinGoClient *gremlingo.Client // Stores the actual gremlin-go client
	log             zerolog.Logger
	mu              sync.RWMutex
	lastErr         error
	// auth CredentialProvider // Storing original auth provider if needed for re-auth logic (not used yet)
	traversalSource string
	isClosed        bool
}

// GremlinGoClientOption defines a function signature for options to customize the GremlinGoClient by configuring ClientSettings.
// These functions are applied to gremlingo.ClientSettings before the client is created.
type GremlinGoClientOption func(settings *gremlingo.ClientSettings) error

// WithGremlinGoAuth sets authentication credentials for the client.
func WithGremlinGoAuth(auth CredentialProvider) GremlinGoClientOption {
	return func(settings *gremlingo.ClientSettings) error {
		if auth == nil {
			return nil // No auth provider, no error
		}
		username, err := auth.Username()
		if err != nil {
			return fmt.Errorf("GremlinGoClientOption: failed to get username: %w", err)
		}
		password, err := auth.Password()
		if err != nil {
			return fmt.Errorf("GremlinGoClientOption: failed to get password: %w", err)
		}
		settings.AuthInfo = gremlingo.BasicAuthInfo(username, password)
		return nil
	}
}

// WithGremlinGoTraversalSource sets the traversal source (e.g., "g") for the client.
func WithGremlinGoTraversalSource(ts string) GremlinGoClientOption {
	return func(settings *gremlingo.ClientSettings) error {
		if ts == "" {
			return fmt.Errorf("GremlinGoClientOption: traversal source cannot be empty")
		}
		settings.TraversalSource = ts
		return nil
	}
}

// WithGremlinGoDriverLogger allows setting a logger for the underlying gremlin-go driver.
// Requires an adapter if the provided logger doesn't match gremlingo.Logger interface.
func WithGremlinGoDriverLogger(logger gremlingo.Logger) GremlinGoClientOption {
    return func(settings *gremlingo.ClientSettings) error {
        settings.Logger = logger
        return nil
    }
}


// NewGremlinGoClient creates a new GremlinGoClient.
// The 'options' parameter here refers to GremlinGoClientOption which configures gremlingo.ClientSettings.
func NewGremlinGoClient(gremlinServerURL string, options ...GremlinGoClientOption) (interfaces.QueryExecutor, error) {
	defaultLogger := log.Output(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()

	ggc := &GremlinGoClient{
		url:             gremlinServerURL,
		log:             defaultLogger, // This is the logger for the GremlinGoClient wrapper itself
		traversalSource: "g",      // Default traversal source, can be overridden
		isClosed:        true,     // Client starts as closed until connection is successful
	}

	// Initialize ClientSettings for the gremlin-go driver
	// Defaults are set by gremlin-go's NewClient if fields are zero,
	// but we set TraversalSource explicitly from our default or option.
	finalClientSettings := gremlingo.ClientSettings{
		TraversalSource: ggc.traversalSource,
	}

	// Apply all provided GremlinGoClientOption functions to finalClientSettings
	for _, option := range options {
		if err := option(&finalClientSettings); err != nil {
			return nil, fmt.Errorf("GremlinGoClient: failed to apply client settings option: %w", err)
		}
	}

	// Update ggc's traversalSource from the potentially modified finalClientSettings
	ggc.traversalSource = finalClientSettings.TraversalSource
	// Note: ggc.auth is not directly used for gremlingo.NewClient connection if AuthInfo is set in finalClientSettings.
	// It's kept if our wrapper needs to manage auth independently later.

	// Create the actual gremlin-go.Client.
	// NewClient takes variadic functions of type func(*ClientSettings).
	// We pass a single function that applies all our accumulated finalClientSettings.
	configFunc := func(s *gremlingo.ClientSettings) {
		*s = finalClientSettings // Copy all configured settings
	}

	gremlinGoDriverClient, err := gremlingo.NewClient(gremlinServerURL, configFunc)
	if err != nil {
		ggc.setLastError(err) // Use ggc's logger via setLastError
		return nil, fmt.Errorf("GremlinGoClient: failed to create gremlingo.Client: %w", err)
	}

	ggc.gremlinGoClient = gremlinGoDriverClient
	ggc.isClosed = false

	ggc.log.Info().Msgf("GremlinGoClient: Successfully created client for %s with traversal source '%s'", gremlinServerURL, ggc.traversalSource)
	return ggc, nil
}

func (c *GremlinGoClient) setLastError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastErr = err
	if err != nil {
		c.log.Error().Err(err).Msg("GremlinGoClient: error recorded")
	}
}

// LastError returns the last error encountered by the client.
func (c *GremlinGoClient) LastError() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastErr
}

// Close closes the gremlingo.Client.
func (c *GremlinGoClient) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.isClosed || c.gremlinGoClient == nil {
		c.log.Info().Msg("GremlinGoClient: Close called but client is already closed or not initialized.")
		return nil
	}

	c.gremlinGoClient.Close() // This does not return an error.
	c.log.Info().Msg("GremlinGoClient: Successfully closed.")
	c.isClosed = true
	c.gremlinGoClient = nil
	return nil
}

// IsConnected checks if the client is considered connected.
func (c *GremlinGoClient) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return !c.isClosed && c.gremlinGoClient != nil
}

// mapGremlinGoResultToResponse converts a gremlingo.Result to an interfaces.Response.
func mapGremlinGoResultToResponse(gremlinResult *gremlingo.Result, requestID string, logger zerolog.Logger) (interfaces.Response, error) {
	if gremlinResult == nil {
		return interfaces.Response{}, fmt.Errorf("cannot map nil gremlin result")
	}

	dataInterface := gremlinResult.GetInterface()
	jsonData, err := json.Marshal(dataInterface)
	if err != nil {
		logger.Error().Err(err).Interface("data", dataInterface).Msg("GremlinGoClient: Failed to marshal result data to JSON")
		return interfaces.Response{}, fmt.Errorf("failed to marshal result data to JSON: %w", err)
	}

	return interfaces.Response{
		RequestID: requestID,
		Status: interfaces.Status{
			Code:    200, // Assuming 200 OK. Errors from ResultSet.GetError() are handled by the caller.
			Message: "",
		},
		Result: interfaces.Result{
			Data: json.RawMessage(jsonData),
			Meta: make(map[string]interface{}),
		},
	}, nil
}

// Execute submits a query and returns the results.
func (c *GremlinGoClient) Execute(query string) ([]interfaces.Response, error) {
	if !c.IsConnected() {
		err := fmt.Errorf("GremlinGoClient: client not connected or closed")
		c.setLastError(err)
		return nil, err
	}

	c.log.Debug().Msgf("GremlinGoClient: Executing query: %s", query)
	resultSet, err := c.gremlinGoClient.Submit(query) // Submit takes variadic bindings, not used here.
	if err != nil {
		c.setLastError(err)
		return nil, fmt.Errorf("GremlinGoClient: failed to submit query: %w", err)
	}

	var responses []interfaces.Response
	requestID := resultSet.GetRequestID()
	if requestID == "" {
		requestID = uuid.New().String()
	}

	resultChannel := resultSet.Channel()
	for gremlinResult := range resultChannel {
		if gremlinResult == nil {
			c.log.Warn().Msg("GremlinGoClient: received nil result from result set channel")
			continue
		}
		resp, mapErr := mapGremlinGoResultToResponse(gremlinResult, requestID, c.log)
		if mapErr != nil {
			c.log.Error().Err(mapErr).Msgf("GremlinGoClient: failed to map result: %v", gremlinResult.GetInterface())
			continue
		}
		responses = append(responses, resp)
	}

	if err = resultSet.GetError(); err != nil {
		c.setLastError(err)
		return responses, fmt.Errorf("GremlinGoClient: error from ResultSet after processing: %w", err)
	}

	c.log.Debug().Msgf("GremlinGoClient: Query execution successful, %d responses generated.", len(responses))
	c.setLastError(nil)
	return responses, nil
}

// ExecuteWithBindings submits a query with bindings.
func (c *GremlinGoClient) ExecuteWithBindings(query string, bindings, rebindings map[string]interface{}) ([]interfaces.Response, error) {
	if !c.IsConnected() {
		err := fmt.Errorf("GremlinGoClient: client not connected or closed for ExecuteWithBindings")
		c.setLastError(err)
		return nil, err
	}

	finalBindings := make(map[string]interface{})
	for k, v := range bindings {
		finalBindings[k] = v
	}
	for k, v := range rebindings {
		finalBindings[k] = v
	}

	optBuilder := gremlingo.RequestOptionsBuilder{}
	optBuilder.SetBindings(finalBindings)
	// optBuilder.SetRequestId(uuid.New()) // Example
	requestOptions := optBuilder.Create()

	c.log.Debug().Str("query", query).Interface("bindings", finalBindings).Msg("GremlinGoClient: Executing query with bindings")

	resultSet, err := c.gremlinGoClient.SubmitWithOptions(query, requestOptions)
	if err != nil {
		c.setLastError(err)
		return nil, fmt.Errorf("GremlinGoClient: failed to submit query with bindings: %w", err)
	}

	var responses []interfaces.Response
	requestID := resultSet.GetRequestID()
	if requestID == "" {
		requestID = uuid.New().String()
	}

	resultChannel := resultSet.Channel()
	for gremlinResult := range resultChannel {
		if gremlinResult == nil {
			c.log.Warn().Msg("GremlinGoClient: received nil result from result set (with bindings)")
			continue
		}
		resp, mapErr := mapGremlinGoResultToResponse(gremlinResult, requestID, c.log)
		if mapErr != nil {
			c.log.Error().Err(mapErr).Msgf("GremlinGoClient: failed to map bound result: %v", gremlinResult.GetInterface())
			continue
		}
		responses = append(responses, resp)
	}

	if err = resultSet.GetError(); err != nil {
		c.setLastError(err)
		return responses, fmt.Errorf("GremlinGoClient: error from ResultSet after processing bound query: %w", err)
	}

	c.log.Debug().Msgf("GremlinGoClient: Query execution with bindings successful, %d responses generated.", len(responses))
	c.setLastError(nil)
	return responses, nil
}

// ExecuteAsync submits a query and streams results asynchronously.
func (c *GremlinGoClient) ExecuteAsync(query string, responseChannel chan interfaces.AsyncResponse) (err error) {
	if !c.IsConnected() {
		return fmt.Errorf("GremlinGoClient: client not connected or closed for async execution")
	}

	c.log.Debug().Msgf("GremlinGoClient: Executing async query: %s", query)

	go func() {
		defer close(responseChannel)

		resultSet, submitErr := c.gremlinGoClient.Submit(query)
		if submitErr != nil {
			c.setLastError(submitErr)
			responseChannel <- interfaces.AsyncResponse{
				ErrorMessage: fmt.Errorf("GremlinGoClient: failed to submit async query: %w", submitErr).Error(),
			}
			return
		}

		requestID := resultSet.GetRequestID()
		if requestID == "" {
			requestID = uuid.New().String()
		}

		resultChannel := resultSet.Channel()
		for gremlinResult := range resultChannel {
			if gremlinResult == nil {
				c.log.Warn().Msg("GremlinGoClient: received nil result from async result set")
				continue
			}

			resp, mapErr := mapGremlinGoResultToResponse(gremlinResult, requestID, c.log)
			if mapErr != nil {
				c.log.Error().Err(mapErr).Msgf("GremlinGoClient: failed to map async result: %v", gremlinResult.GetInterface())
				responseChannel <- interfaces.AsyncResponse{ErrorMessage: mapErr.Error()}
				continue
			}
			responseChannel <- interfaces.AsyncResponse{Response: resp}
		}

		if iterErr := resultSet.GetError(); iterErr != nil {
			c.setLastError(iterErr)
			responseChannel <- interfaces.AsyncResponse{
				ErrorMessage: fmt.Errorf("GremlinGoClient: error from ResultSet after async processing: %w", iterErr).Error(),
			}
		}
		c.log.Debug().Msgf("GremlinGoClient: Async query execution finished for query: %s", query)
	}()

	return nil
}

// ExecuteFile reads a query from a file and executes it.
func (c *GremlinGoClient) ExecuteFile(path string) ([]interfaces.Response, error) {
	c.log.Debug().Msgf("GremlinGoClient: Attempting to execute query from file: %s", path)
	queryBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("GremlinGoClient: failed to read query file %s: %w", path, err)
	}
	return c.Execute(string(queryBytes))
}

// ExecuteFileWithBindings reads a query from a file and executes it with bindings.
func (c *GremlinGoClient) ExecuteFileWithBindings(path string, bindings, rebindings map[string]interface{}) ([]interfaces.Response, error) {
	c.log.Debug().Msgf("GremlinGoClient: Attempting to execute query with bindings from file: %s", path)
	queryBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("GremlinGoClient: failed to read query file %s for execution with bindings: %w", path, err)
	}
	return c.ExecuteWithBindings(string(queryBytes), bindings, rebindings)
}

// Ping sends a simple query to check connectivity.
func (c *GremlinGoClient) Ping() error {
	if !c.IsConnected() {
		err := fmt.Errorf("GremlinGoClient: client not connected or closed for ping")
		c.setLastError(err)
		return err
	}

	pingQuery := "'ping'"
	c.log.Debug().Msgf("GremlinGoClient: Pinging with query: %s", pingQuery)

	resultSet, err := c.gremlinGoClient.Submit(pingQuery)
	if err != nil {
		c.setLastError(err)
		return fmt.Errorf("GremlinGoClient: ping query submission failed: %w", err)
	}

	var foundResult bool
	resultChannel := resultSet.Channel()
	for gremlinResult := range resultChannel {
		if gremlinResult == nil {
			c.log.Warn().Msg("GremlinGoClient: ping received nil result from channel")
			continue
		}
		c.log.Debug().Str("ping_result", gremlinResult.GetString()).Msg("GremlinGoClient: Ping result received")
		foundResult = true
	}

	if err = resultSet.GetError(); err != nil {
		c.setLastError(err)
		return fmt.Errorf("GremlinGoClient: error from ResultSet after ping: %w", err)
	}

	if !foundResult && err == nil {
	    err = fmt.Errorf("GremlinGoClient: ping query returned no results and no explicit error from ResultSet")
		c.setLastError(err)
		return err
	}

	c.log.Info().Msg("GremlinGoClient: Ping successful.")
	c.setLastError(nil)
	return nil
}

// DialGremlinGo creates and returns a new QueryExecutor based on the GremlinGoClient.
// It serves as a factory function for the GremlinGoClient.
// The 'options' parameter configures the underlying gremlingo.ClientSettings.
func DialGremlinGo(url string, options ...GremlinGoClientOption) (interfaces.QueryExecutor, error) {
	// NewGremlinGoClient already sets up the logger internally for the wrapper.
	// Options are passed through to configure gremlingo.ClientSettings.
	client, err := NewGremlinGoClient(url, options...)
	if err != nil {
		// NewGremlinGoClient logs the error internally before returning it.
		// We can add context here if needed.
		return nil, fmt.Errorf("DialGremlinGo: failed to create GremlinGoClient: %w", err)
	}
	return client, nil
}
