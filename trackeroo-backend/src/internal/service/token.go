package service

import (
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"
	"trackeroo-backend/internal/model"

	jwt "github.com/golang-jwt/jwt/v5"
)

func GenerateJWT(user model.User) (string, jwt.MapClaims, error) {
	claims := jwt.MapClaims{
		"user_id": user.ID,
		"role":    user.Role,
		"exp":     time.Now().Add(time.Hour * 24).Unix(), // Expires in 24h
		"iat":     time.Now().Unix(),
	}
	jwtKey := getKey("users_key")
	if jwtKey == nil {
		return "", nil, fmt.Errorf("failed to get jwt key")
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(jwtKey)
	if err != nil {
		return "", nil, err
	}

	return tokenString, claims, nil
}

func ValidateUserJWT(authHeader string, role string) (bool, error) {
	parts := strings.SplitN(authHeader, " ", 2)
	if len(parts) != 2 {
		return false, fmt.Errorf("invalid Authorization header format")
	}
	token, err := jwt.Parse(parts[1], func(token *jwt.Token) (any, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return getKey("users_key"), nil
	})
	if err != nil {
		return false, err
	}

	if !token.Valid || token.Claims.(jwt.MapClaims)["role"] != role {
		return false, fmt.Errorf("invalid token")
	}

	return true, nil
}

func ValidateDeviceJWT(encodedKey string, tokenString string) (bool, jwt.MapClaims, error) {
	secret, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil {
		return false, nil, fmt.Errorf("invalid base64 key: %w", err)
	}

	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return false, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return secret, nil
	})
	if err != nil {
		return false, nil, err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		return true, claims, nil
	}

	return false, nil, fmt.Errorf("invalid token")
}
func fileExists(path string) bool {
	_, err := os.Stat(path)
	if err == nil {
		return true
	}
	if os.IsNotExist(err) {
		return false
	}
	return false
}
func getKey(key string) []byte {
	run_path := fmt.Sprintf("/run/secrets/%s", key)
	app_path := fmt.Sprintf("/app/secrets/%s", key)
	var jwtKey []byte
	var err error
	if fileExists(run_path) {
		jwtKey, err = os.ReadFile(run_path)
		if err != nil {
			return nil
		}
	} else if fileExists(app_path) {
		jwtKey, err = os.ReadFile(app_path)
		if err != nil {
			return nil
		}
	} else {
		return nil
	}
	return jwtKey
}

func GenPrivateKey() (string, error) {
	privateKey := make([]byte, 32)
	_, err := rand.Read(privateKey)
	if err != nil {
		return "", err
	}
	encodedKey := base64.StdEncoding.EncodeToString(privateKey)
	return encodedKey, nil
}
