package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
	"trackeroo-backend/internal/config"
	"trackeroo-backend/internal/handler"
	"trackeroo-backend/internal/logger"
	"trackeroo-backend/internal/middleware"
	"trackeroo-backend/internal/router"
	"trackeroo-backend/internal/service"
)

func main() {

	logger.InitLogger()
	fmt.Println(service.Art)
	config.LoadConfig()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := service.InitDb(ctx); err != nil {
		logger.Fatal(err.Error())
	}

	logger.Debug("Initializing login route")
	loginRouter := router.NewRouter().
		AddHandler("POST /", handler.HandleLogin).
		Finalize()

	logger.Debug("Initializing devices route")
	devicesRouter := router.NewRouter().
		AddHandler("GET /", handler.GetDevices).
		AddHandler("POST /", handler.CreateDevice).
		AddHandler("GET /{id}", handler.GetDevice).
		AddHandler("GET /credentials", handler.GetCredentials).
		AddHandler("GET /{id}/credentials", handler.GetDeviceCredentials).
		AddHandler("PUT /{id}", handler.UpdateDevice).
		AddHandler("DELETE /{id}", handler.DeleteDevice).
		AddMiddleware(middleware.Auth).
		Finalize()

	logger.Debug("Initializing users router")
	usersRouter := router.NewRouter().
		AddHandler("GET /", handler.GetUsers).
		AddHandler("POST /", handler.CreateUser).
		AddHandler("GET /{id}", handler.GetUser).
		AddHandler("PUT /{id}", handler.UpdateUser).
		AddHandler("DELETE /{id}", handler.DeleteUser).
		AddMiddleware(middleware.Auth).
		Finalize()

	logger.Debug("Initializing auth router")
	authRouter := router.NewRouter().
		AddHandler("POST /user", handler.UserAuth).
		AddHandler("POST /vhost", handler.VhostAuth).
		AddHandler("POST /resource", handler.ResourceAuth).
		AddHandler("POST /topic", handler.TopicAuth).
		Finalize()

	logger.Debug("Initializing main route")
	mainRouter := router.NewRouter().
		AddHandler("GET /health", handler.HealthCheck).
		AddMiddleware(middleware.Logging).
		AddMiddleware(middleware.Recover).
		AddSubroute("/login/", loginRouter).
		AddSubroute("/devices/", devicesRouter).
		AddSubroute("/users/", usersRouter).
		AddSubroute("/auth/", authRouter).
		Finalize()

	server := http.Server{
		Addr:    ":8080",
		Handler: mainRouter,
	}
	logger.Info("Starting on port 8080")
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal(err.Error())
		}
	}()

	<-stop // wait for interrupt
	logger.Info("Shutting down server...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	// Shutdown HTTP server
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Fatal("Server shutdown failed: %s", err.Error())
	}

	// Disconnect MongoDB
	if err := service.CloseDb(shutdownCtx); err != nil {
		logger.Fatal("MongoDB disconnect failed: %s", err.Error())
	}
}
