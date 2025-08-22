package service

import (
	"context"
	"errors"
	"os"

	"trackeroo-backend/internal/model"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var MongoClient *mongo.Client

func InitDb(ctx context.Context) error {
	var err error
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		return errors.New("MONGO_URI not set in environment")
	}

	MongoClient, err = mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		return err
	}

	err = EnsureUsers(ctx)
	return err
}

func CloseDb(ctx context.Context) error {
	return MongoClient.Disconnect(ctx)
}

func InsertUser(ctx context.Context, user model.User) error {
	collection := MongoClient.Database("trackeroo-backend").Collection("users")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	_, err := collection.InsertOne(_ctx, user)
	return err
}

func GetUser(ctx context.Context, id string) (model.User, error) {
	collection := MongoClient.Database("trackeroo-backend").Collection("users")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	user := model.User{}
	err := collection.FindOne(_ctx, bson.M{"_id": id}).Decode(&user)
	return user, err
}

func GetUserByName(ctx context.Context, name string) (model.User, error) {
	collection := MongoClient.Database("trackeroo-backend").Collection("users")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	user := model.User{}
	err := collection.FindOne(_ctx, bson.M{"name": name}).Decode(&user)
	return user, err
}

func GetUsers(ctx context.Context) ([]model.User, error) {
	collection := MongoClient.Database("trackeroo-backend").Collection("users")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	cursor, err := collection.Find(_ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(_ctx)
	users := []model.User{}
	for cursor.Next(_ctx) {
		var user model.User
		if err := cursor.Decode(&user); err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	return users, nil
}

func UpdateUser(ctx context.Context, user model.User) error {
	collection := MongoClient.Database("trackeroo-backend").Collection("users")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	_, err := collection.UpdateOne(_ctx, bson.M{"_id": user.ID}, bson.M{"$set": user})
	return err
}

func DeleteUser(ctx context.Context, id string) error {
	collection := MongoClient.Database("trackeroo-backend").Collection("users")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	_, err := collection.DeleteOne(_ctx, bson.M{"_id": id})
	return err
}

func InsertDevice(ctx context.Context, id string, device model.Device) error {
	collection := MongoClient.Database("trackeroo-backend").Collection("devices")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	_, err := collection.InsertOne(_ctx, device)
	return err
}

func GetDevice(ctx context.Context, id string) (model.Device, error) {
	collection := MongoClient.Database("trackeroo-backend").Collection("devices")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	device := model.Device{}
	err := collection.FindOne(_ctx, bson.M{"_id": id}).Decode(&device)
	return device, err
}

func GetDevices(ctx context.Context) ([]model.Device, error) {
	collection := MongoClient.Database("trackeroo-backend").Collection("devices")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	cursor, err := collection.Find(_ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(_ctx)
	devices := []model.Device{}
	for cursor.Next(_ctx) {
		var device model.Device
		if err := cursor.Decode(&device); err != nil {
			return nil, err
		}
		devices = append(devices, device)
	}
	return devices, nil
}

func UpdateDevice(ctx context.Context, id string, device model.Device) error {
	collection := MongoClient.Database("trackeroo-backend").Collection("devices")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	_, err := collection.UpdateOne(_ctx, bson.M{"_id": id}, bson.M{"$set": device})
	return err
}

func DeleteDevice(ctx context.Context, id string) error {
	collection := MongoClient.Database("trackeroo-backend").Collection("devices")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	_, err := collection.DeleteOne(_ctx, bson.M{"_id": id})
	return err
}

func EnsureUsers(ctx context.Context) error {
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	users := model.DefaultUsers()
	for _, user := range users {
		/* If the error is nil it means the user already exists */
		if _, err := GetUserByName(_ctx, user.Username); err == nil {
			continue
		}
		if err := InsertUser(_ctx, user); err != nil {
			return err
		}
	}
	return nil
}
