// Package cloberrors provides error mapping utilities for the Polymarket CLOB.
// It converts generic HTTP errors from the transport layer into structured,
// recognizable error types from pkg/errors for programmatic error handling.
package cloberrors

import (
	"errors"
	"fmt"
	"strings"

	sdkerrors "github.com/GoPolymarket/polymarket-go-sdk/v2/pkg/errors"
	"github.com/GoPolymarket/polymarket-go-sdk/v2/pkg/types"
)

type mappedResponseError struct {
	mapped   error
	response *types.Error
}

func (e *mappedResponseError) Error() string { return e.mapped.Error() }

func (e *mappedResponseError) Unwrap() error { return e.mapped }

func (e *mappedResponseError) Is(target error) bool {
	return errors.Is(e.response, target)
}

func (e *mappedResponseError) As(target any) bool {
	if response, ok := target.(**types.Error); ok {
		*response = e.response
		return true
	}
	return false
}

// FromTypeErr maps a generic types.Error (from transport layer) to a specific
// structured error type from pkg/errors.
func FromTypeErr(err *types.Error) (result error) {
	if err == nil {
		return nil
	}
	// Keep the existing mapped sentinel and human-readable message, but retain
	// the transport's complete status/path/code evidence for safe classification.
	defer func() {
		if err.Status == 400 && result != nil && !errors.Is(result, err) {
			result = &mappedResponseError{mapped: result, response: err}
		}
	}()

	// Map by Code if available (most reliable)
	code := strings.ToUpper(err.Code)
	switch code {
	case "INSUFFICIENT_FUNDS", "INSUFFICIENT_BALANCE", "INSUFFICIENT_ALLOWANCE":
		return fmt.Errorf("%w: %s", sdkerrors.ErrInsufficientFunds, err.Message)
	case "INVALID_SIGNATURE", "AUTH_INVALID_SIGNATURE":
		return fmt.Errorf("%w: %s", sdkerrors.ErrInvalidSignature, err.Message)
	case "ORDER_NOT_FOUND":
		return fmt.Errorf("%w: %s", sdkerrors.ErrOrderNotFound, err.Message)
	case "MARKET_CLOSED":
		return fmt.Errorf("%w: %s", sdkerrors.ErrMarketClosed, err.Message)
	case "GEOBLOCKED":
		return fmt.Errorf("%w: %s", sdkerrors.ErrGeoblocked, err.Message)
	case "INVALID_PRICE":
		return fmt.Errorf("%w: %s", sdkerrors.ErrInvalidPrice, err.Message)
	case "INVALID_SIZE":
		return fmt.Errorf("%w: %s", sdkerrors.ErrInvalidSize, err.Message)
	}

	// Fallback mapping by Status
	switch err.Status {
	case 401:
		return fmt.Errorf("%w: %s", sdkerrors.ErrUnauthorized, err.Message)
	case 403:
		if strings.Contains(strings.ToUpper(err.Message), "GEO") {
			return fmt.Errorf("%w: %s", sdkerrors.ErrGeoblocked, err.Message)
		}
		return fmt.Errorf("%w: %s", sdkerrors.ErrUnauthorized, err.Message)
	case 400:
		return fmt.Errorf("%w: %s", sdkerrors.ErrBadRequest, err.Message)
	case 429:
		return sdkerrors.ErrRateLimitExceeded
	case 500, 502, 503, 504:
		return fmt.Errorf("%w: %s", sdkerrors.ErrInternalServerError, err.Message)
	}

	return err
}
