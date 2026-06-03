// Carried/trimmed from github.com/fil-forge/guppy/pkg/client/options.go.
package forgeclient

import (
	"github.com/fil-forge/ingot/tokenstore"
	"github.com/fil-forge/libforge/receipt"
	"github.com/fil-forge/ucantone/client"
	"go.uber.org/zap"
)

// Option configures a Client.
type Option func(c *Client) error

// WithUCANClientOptions passes options to the underlying UCAN HTTP client.
func WithUCANClientOptions(options ...client.HTTPOption) Option {
	return func(c *Client) error {
		c.ucanOpts = append(c.ucanOpts, options...)
		return nil
	}
}

// WithReceiptsClient overrides the receipt-polling client.
func WithReceiptsClient(receiptsClient *receipt.Client) Option {
	return func(c *Client) error {
		c.receiptsClient = receiptsClient
		return nil
	}
}

// WithTokenStore sets the delegation/attestation store. Defaults to an
// in-memory store when unset.
func WithTokenStore(store tokenstore.Store) Option {
	return func(c *Client) error {
		c.tokenStore = store
		return nil
	}
}

// WithLogger sets the client's logger (defaults to a nop logger).
func WithLogger(l *zap.Logger) Option {
	return func(c *Client) error {
		if l != nil {
			c.logger = l
		}
		return nil
	}
}
