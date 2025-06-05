package gremcos

import (
	"errors"
	"fmt"
	"net/url"
	"sync"

	gremlingo "github.com/apache/tinkerpop/gremlin-go/v3/driver"
	"github.com/rs/zerolog/log"

	"github.com/ameena3/gremcos/interfaces"
)

// GremlinGoDialer implements the Dialer interface for gremlin-go driver.
type GremlinGoDialer struct {
	url      string
	conn     *gremlingo.DriverRemoteConnection
	mu       sync.RWMutex
	isClosed bool
}

// NewGremlinGoDialer creates a new GremlinGoDialer.
func NewGremlinGoDialer(host string) (interfaces.Dialer, error) {
	u, err := url.Parse(host)
	if err != nil {
		return nil, fmt.Errorf("failed to parse host URL: %w", err)
	}
	// The gremlin-go driver expects a ws/wss scheme.
	// It defaults to "ws" if not specified, but let's ensure it's what we expect.
	if u.Scheme != "ws" && u.Scheme != "wss" {
		log.Warn().Msgf("GremlinGoDialer: host scheme is %s, changing to ws for gremlin-go driver compatibility", u.Scheme)
		// Default to "ws" if an http/https scheme was provided, common for gremcos
		if u.Scheme == "http" {
			u.Scheme = "ws"
		} else if u.Scheme == "https" {
			u.Scheme = "wss"
		} else {
			// If it's neither http/https nor ws/wss, it's an unsupported scheme.
			return nil, fmt.Errorf("unsupported URL scheme for GremlinGoDialer: %s. Use ws or wss", u.Scheme)
		}
	}


	return &GremlinGoDialer{
		url:      u.String(),
		isClosed: true,
	}, nil
}

// Connect establishes a connection using gremlingo.NewDriverRemoteConnection().
func (g *GremlinGoDialer) Connect() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if !g.isClosed && g.conn != nil {
		// Potentially check if connection is actually alive if driver provides such a mechanism
		log.Info().Msg("GremlinGoDialer: Connect called but connection already exists and is not marked closed.")
		return nil
	}

	var err error
	g.conn, err = gremlingo.NewDriverRemoteConnection(g.url)
	if err != nil {
		return fmt.Errorf("GremlinGoDialer: failed to create driver remote connection: %w", err)
	}
	g.isClosed = false
	log.Info().Msgf("GremlinGoDialer: Connection established to %s", g.url)
	return nil
}

// IsConnected checks if the connection is established and not closed.
func (g *GremlinGoDialer) IsConnected() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	// gremlin-go's DriverRemoteConnection does not have an explicit IsConnected method.
	// We rely on our isClosed flag and the presence of the conn object.
	// A more robust check might involve trying to get session information or similar if available.
	return g.conn != nil && !g.isClosed
}

// Close closes the gremlin-go connection.
func (g *GremlinGoDialer) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.isClosed || g.conn == nil {
		log.Info().Msg("GremlinGoDialer: Close called but connection already closed or not initialized.")
		return nil
	}

	g.conn.Close()
	// Assuming g.conn.Close() does not return an error.
	// If it can panic or has other failure modes not expressed as a return error,
	// this might need more sophisticated handling or checks post-closure.
	g.isClosed = true
	g.conn = nil // Release the connection object
	log.Info().Msg("GremlinGoDialer: Connection closed.")
	return nil
}

// Write is not directly applicable for GremlinGoDialer as gremlin-go handles serialization.
func (g *GremlinGoDialer) Write(msg []byte) error {
	log.Warn().Msg("GremlinGoDialer: Write method called. This method is not supported as gremlin-go handles message serialization and formatting internally. Use specific gremlin-go client methods to send queries.")
	return errors.New("GremlinGoDialer: Write method not supported/applicable. Use gremlin-go client's submission methods")
}

// Read is not directly applicable for GremlinGoDialer.
func (g *GremlinGoDialer) Read() (int, []byte, error) {
	log.Warn().Msg("GremlinGoDialer: Read method called. This method is not supported as gremlin-go handles message deserialization internally. Results are obtained via specific client methods.")
	return 0, nil, errors.New("GremlinGoDialer: Read method not supported/applicable. Use gremlin-go client's result handling")
}

// Ping checks the connection status.
// gremlin-go's DriverRemoteConnection does not have an explicit Ping method.
// This implementation considers the connection active if IsConnected returns true.
func (g *GremlinGoDialer) Ping() error {
	if !g.IsConnected() {
		return errors.New("GremlinGoDialer: Ping failed, not connected")
	}
	// A more active ping could involve sending a simple query like "1+1"
	// and checking for a successful response, but that's beyond a simple ping.
	// For now, relying on IsConnected status.
	log.Info().Msg("GremlinGoDialer: Ping successful (connection considered active).")
	return nil
}

// GetAuth returns nil as GremlinGoDialer handles auth via connection string.
// The Dialer interface might need to be adjusted or this dialer might not fully conform if Auth is strictly required by the interface in a different form.
func (g *GremlinGoDialer) GetAuth() (CredentialProvider, error) {
	return nil, nil
}

// SetAuth is a no-op as GremlinGoDialer handles auth via connection string.
func (g *GremlinGoDialer) SetAuth(auth CredentialProvider) error {
	log.Info().Msg("GremlinGoDialer: SetAuth called, but authentication is handled via the connection URL for gremlin-go.")
	return nil
}

// SetTimeout is a no-op. Timeout configuration for gremlin-go is typically part of connection options.
func (g *GremlinGoDialer) SetTimeout(timeout int) error {
	log.Info().Msg("GremlinGoDialer: SetTimeout called. Connection timeouts for gremlin-go are typically configured at a deeper level (e.g., connection pool settings if applicable) or via connection parameters, not directly on the Dialer like this.")
	return nil
}

// SetHeader is a no-op. Custom headers are not typically managed this way with gremlin-go.
func (g *GremlinGoDialer) SetHeader(key, value string) error {
	log.Info().Msg("GremlinGoDialer: SetHeader called. Custom headers for gremlin-go requests are managed differently, if at all supported by the driver for general requests beyond initial connection setup.")
	return nil
}

// UnderlyingConn returns the raw gremlin-go DriverRemoteConnection.
// This is specific to GremlinGoDialer and allows access to the driver's capabilities.
func (g *GremlinGoDialer) UnderlyingConn() *gremlingo.DriverRemoteConnection {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.conn
}
