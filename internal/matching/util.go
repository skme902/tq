package matching

import "log/slog"

// softAssert replaces go.temporal.io/server/common/softassert.That: it logs (never panics)
// when cond is false and returns cond.
func softAssert(logger *slog.Logger, cond bool, msg string) bool {
	if !cond && logger != nil {
		logger.Error("soft assertion failed: " + msg)
	}
	return cond
}
