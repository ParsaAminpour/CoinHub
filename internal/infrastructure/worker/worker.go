package worker

import (
	"coinhub/internal"
	"coinhub/internal/adapter/tasks"
	"coinhub/internal/infrastructure/configs"
	"context"

	"github.com/getsentry/sentry-go"
	"github.com/hibiken/asynq"
)

func NewWorker(ctx context.Context, conf configs.Configuration) *asynq.Server {
	server := asynq.NewServer(
		asynq.RedisClientOpt{
			Addr:     conf.RedisAddress(),
			Password: conf.Storage.Redis.Password,
			DB:       conf.Service.QueueDB,
		},
		asynq.Config{
			Concurrency:    10,
			StrictPriority: true,
			Queues: map[string]int{
				"transaction": 5,
				"email":       5,
			},
			ErrorHandler: asynq.ErrorHandlerFunc(func(ctx context.Context, task *asynq.Task, err error) {
				sentry.ConfigureScope(func(scope *sentry.Scope) {
					scope.SetContext("Task", map[string]interface{}{
						"Payload": task.Payload(),
						"Type":    task.Type(),
					})
				})
				if conf.App.Env == "PRODUCTION" {
					sentry.CaptureException(err)
				}
			}),
		})
	return server
}

func RegisterWorkerHandler(mux *asynq.ServeMux, app *internal.Application) {
	mux.HandleFunc(tasks.EvmTransactionUpdateStatusV1, func(ctx context.Context, task *asynq.Task) error {
		return tasks.HandleTransactionStatus(ctx, task, app.MySqlGorm, app.AsynqClient, app.ETHClient, &app.TransactinRepository)
	})
	mux.HandleFunc(tasks.UserUpdateEmailVerificaitonV1, func(ctx context.Context, task *asynq.Task) error {
		return tasks.HandleEmailVerificationCodeSendOp(ctx, task, app.RedisClient, app.MailDialer, app.AuthGmailCache)
	})
}
