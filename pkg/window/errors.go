package window

import "errors"

// ErrInvalidSize is returned when a window spec has a non-positive size.
var ErrInvalidSize = errors.New("window: size must be positive")
