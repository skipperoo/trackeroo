package middleware

import (
	"net/http"
	"time"
	"trackeroo-backend/internal/logger"
)

type wrappedWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *wrappedWriter) WriteHeader(statusCode int) {
	w.ResponseWriter.WriteHeader(statusCode)
	w.statusCode = statusCode
}

func Logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrapped := &wrappedWriter{
			ResponseWriter: w,
			statusCode:     http.StatusOK,
		}
		next.ServeHTTP(wrapped, r)
		if wrapped.statusCode < 400 {
			logger.Info("%v %v %v %v", wrapped.statusCode, r.Method, r.URL.Path, time.Since(start))
		} else if wrapped.statusCode < 500 {
			logger.Warning("%v %v %v %v", wrapped.statusCode, r.Method, r.URL.Path, time.Since(start))
		} else {
			logger.Error("%v %v %v %v", wrapped.statusCode, r.Method, r.URL.Path, time.Since(start))
		}
	})
}
