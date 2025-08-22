package service

import (
	"fmt"
	"os"
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

func ValidateJWT(tokenString string, role string) (bool, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
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

func getKey(key string) []byte {
	jwtKey, err := os.ReadFile(fmt.Sprintf("/run/secrets/%s", key))
	if err != nil {
		return nil
	}
	return jwtKey
}
