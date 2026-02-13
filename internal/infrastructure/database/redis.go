package database

// func NewRedisClient(ctx context.Context, config configs.Configuration) error {
// 	client := redis.NewClient(&redis.Options{
// 		Addr:     config.RedisAddress(),
// 		Password: config.Storage.Redis.Password,
// 		DB:       config.Service.CacheDB,
// 	})

// 	if _, err := client.Ping(ctx).Result(); err != nil {
// 		return err
// 	}

// 	app.RedisClient = client
// 	return nil
// }
