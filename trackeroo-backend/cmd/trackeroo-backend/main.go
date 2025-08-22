package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"trackeroo-backend/internal/handler"
	"trackeroo-backend/internal/middleware"
	"trackeroo-backend/internal/router"
	"trackeroo-backend/internal/service"
)

func main() {

	service.InitLogger(service.DEBUG, "")
	fmt.Println(service.Art)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := service.InitDb(ctx); err != nil {
		service.Fatal(err.Error())
	}

	/**
	 	* This creates the first admin router that will be
		* added as a subroute to the main router.
		* Here you can add All the handlers and middlewares you want,
		* in particular the endpoints here will be restricted to admins.
		*
	*/
	service.Debug("Initializing login route")
	loginRouter := router.NewRouter().
		AddHandler("POST /", handler.HandleLogin).
		Finalize()

	service.Debug("Initializing devices route")
	devicesRouter := router.NewRouter().
		AddHandler("GET /", handler.GetDevices).
		AddHandler("POST /", handler.CreateDevice).
		AddHandler("GET /{id}", handler.GetDevice).
		AddHandler("PUT /{id}", handler.UpdateDevice).
		AddHandler("DELETE /{id}", handler.DeleteDevice).
		AddMiddleware(middleware.Auth).
		Finalize()

	service.Debug("Initializing users router")
	usersRouter := router.NewRouter().
		AddHandler("GET /", handler.GetUsers).
		AddHandler("POST /", handler.CreateUser).
		AddHandler("GET /{id}", handler.GetUser).
		AddHandler("PUT /{id}", handler.UpdateUser).
		AddHandler("DELETE /{id}", handler.DeleteUser).
		// AddMiddleware(middleware.Auth).
		Finalize()

	/**
	 	* Now, this is the main router:
		* it will provide the Logging middleware for all the subroutes
		* and will be passed to the http server.
		* Its middleware will be executed BEFORE the subroute middlewares
	*/
	service.Debug("Initializing main route")
	mainRouter := router.NewRouter().
		AddHandler("GET /health", handler.HealthCheck).
		AddMiddleware(middleware.Logging).
		AddSubroute("/login/", loginRouter).
		AddSubroute("/devices/", devicesRouter).
		AddSubroute("/users/", usersRouter).
		Finalize()

	server := http.Server{
		Addr:    ":8081",
		Handler: mainRouter,
	}
	service.Info("Starting on port 8081")
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			service.Fatal(err.Error())
		}
	}()

	<-stop // wait for interrupt
	service.Info("Shutting down server...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	// Shutdown HTTP server
	if err := server.Shutdown(shutdownCtx); err != nil {
		service.Fatal("Server shutdown failed: %s", err.Error())
	}

	// Disconnect MongoDB
	if err := service.CloseDb(shutdownCtx); err != nil {
		service.Fatal("MongoDB disconnect failed: %s", err.Error())
	}
}
