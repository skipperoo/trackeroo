package router

import (
	"net/http"
	"strings"
	"trackeroo-backend/internal/service"
)

type handler struct {
	Endpoint string
	fn       http.HandlerFunc
}

type middleware func(http.Handler) http.Handler
type subroute struct {
	Path   string
	Handle http.Handler
}

type Router struct {
	r           *http.ServeMux
	handlers    []handler
	middlewares []middleware
	subroutes   []subroute
}

func NewRouter() *Router {
	router := http.NewServeMux()

	return &Router{r: router}
}

func (r *Router) AddHandler(endpoint string, fn http.HandlerFunc) *Router {
	r.handlers = append(r.handlers, handler{endpoint, fn})
	return r
}

func (r *Router) AddMiddleware(mw middleware) *Router {
	r.middlewares = append(r.middlewares, mw)
	return r
}

func (r *Router) AddSubroute(path string, handler http.Handler) *Router {
	r.subroutes = append(r.subroutes, subroute{path, handler})
	return r
}

func createStack(mw []middleware) middleware {
	service.Debug("Creating middleware stack with %d middlewares", len(mw))
	return func(next http.Handler) http.Handler {
		service.Debug("Applying middleware stack to handler")
		for i := len(mw) - 1; i >= 0; i-- {
			x := mw[i]
			service.Debug("Applying middleware %+v", x)
			next = x(next)
		}
		return next
	}
}

func (r *Router) Finalize() http.Handler {
	for _, handler := range r.handlers {
		r.r.HandleFunc(handler.Endpoint, handler.fn)
		service.Debug("Adding handler: %s", handler.Endpoint)
	}
	for _, subroute := range r.subroutes {
		// This removes the trailing /
		prefix := strings.TrimSuffix(subroute.Path, "/")
		// This crates an handler that removes the /subroute prefix
		strippedHandler := http.StripPrefix(prefix, subroute.Handle)

		service.Debug("Adding subroute: %s", subroute.Path)
		r.r.Handle(subroute.Path, strippedHandler)
	}
	stack := createStack(r.middlewares)

	return stack(r.r)
}
