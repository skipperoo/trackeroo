package service

import (
	"context"
	"errors"
	"fmt"

	"trackeroo-backend/internal/config"
	"trackeroo-backend/internal/logger"
	"trackeroo-backend/internal/model"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var MongoClient *mongo.Client

var keysCache = NewSimpleCache()
var devicesCache = NewSimpleCache()
var credentialsCache = NewSimpleCache()
var usersCache = NewSimpleCache()
var isUserCache = NewSimpleCache()

func InitDb(ctx context.Context) error {
	var err error
	mongoURI := config.Cfg.MongoUri
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
	cacheEntry := usersCache.Get(id)
	if cacheEntry != nil {
		user, ok := cacheEntry.(model.User)
		if ok {
			return user, nil
		}
	}
	collection := MongoClient.Database("trackeroo-backend").Collection("users")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	user := model.User{}
	objID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return model.User{}, err
	}
	err = collection.FindOne(_ctx, bson.M{"_id": objID}).Decode(&user)
	usersCache.Set(id, user)
	return user, err
}

func GetUserByName(ctx context.Context, name string) (model.User, error) {
	if val, ok := isUserCache.Get(name).(bool); ok && !val {
		return model.User{}, fmt.Errorf("%s is not a user", name)
	}
	cacheEntry := usersCache.Get(name)
	if cacheEntry != nil {
		user, ok := cacheEntry.(model.User)
		if ok {
			return user, nil
		}
	}
	collection := MongoClient.Database("trackeroo-backend").Collection("users")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	user := model.User{}
	err := collection.FindOne(_ctx, bson.M{"username": name}).Decode(&user)
	if err == nil {
		usersCache.Set(name, user)
		isUserCache.Set(name, true)
	} else {
		isUserCache.Set(name, false)
	}
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
	usersCache.Invalidate(user.ID)
	usersCache.Invalidate(user.Username)
	return err
}

func DeleteUser(ctx context.Context, id string) error {
	collection := MongoClient.Database("trackeroo-backend").Collection("users")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	_, err := collection.DeleteOne(_ctx, bson.M{"_id": id})
	usersCache.Invalidate(id)
	return err
}

func InsertDevice(ctx context.Context, device model.Device) (model.Device, error) {
	collection := MongoClient.Database("trackeroo-backend").Collection("devices")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	device.ID = model.GenDeviceID()
	res, err := collection.InsertOne(_ctx, device)
	if err != nil {
		return model.Device{}, err
	}
	_id := res.InsertedID.(string)
	dev, err := GetDevice(ctx, _id)
	return dev, err
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

func GetDeviceCredentials(ctx context.Context, id string) (model.DeviceCredentials, error) {
	cacheEntry := credentialsCache.Get(id)
	if cacheEntry != nil {
		creds, ok := cacheEntry.(model.DeviceCredentials)
		if ok {
			return creds, nil
		}
	}

	collection := MongoClient.Database("trackeroo-backend").Collection("devices")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}

	device := model.Device{}
	err := collection.FindOne(_ctx, bson.M{"_id": id}).Decode(&device)
	if err != nil {
		return model.DeviceCredentials{}, err
	}

	creds := model.DeviceCredentials{
		ID:         device.ID,
		Name:       device.Name,
		DeviceType: device.DeviceType,
		PrivateKey: device.PrivateKey,
	}
	credentialsCache.Set(id, creds)
	return creds, nil
}

func GetDeviceKey(ctx context.Context, id string) (string, error) {
	key := keysCache.Get(id)
	if key != nil {
		return key.(string), nil
	}

	collection := MongoClient.Database("trackeroo-backend").Collection("devices")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}

	device := model.Device{}
	err := collection.FindOne(_ctx, bson.M{"_id": id}).Decode(&device)
	if err != nil {
		return "", err
	}
	key = device.PrivateKey
	keysCache.Set(id, key)
	return key.(string), nil
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

	devicesCache.Invalidate(id)
	return err
}

func DeleteDevice(ctx context.Context, id string) error {
	collection := MongoClient.Database("trackeroo-backend").Collection("devices")
	_ctx := ctx
	if _ctx == nil {
		_ctx = context.Background()
	}
	objID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return err
	}
	_, err = collection.DeleteOne(_ctx, bson.M{"_id": objID})
	if err != nil {
		logger.Warning("Error deleting device: %v", err)
	}
	devicesCache.Invalidate(id)
	return nil
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
