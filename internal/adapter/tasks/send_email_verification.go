package tasks

import (
	"coinhub/internal/adapter/mail"
	"coinhub/internal/adapter/repository/cache"
	"coinhub/internal/domain/services"
	"context"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/hibiken/asynq"
	"go.uber.org/zap"
	"gopkg.in/gomail.v2"
)

type EmailVerificationPayload struct {
	RequestTime   string
	IP            string
	Location      string
	Device        string
	Year          string
	ReceiverEmail string
	Username      string
}

func NewEmailVerificationPayload(requestTime, ip, location, device, year, receiverEmail, username string) (*asynq.Task, error) {
	payload, err := json.Marshal(EmailVerificationPayload{
		RequestTime:   requestTime,
		IP:            ip,
		Location:      location,
		Device:        device,
		Year:          year,
		ReceiverEmail: receiverEmail,
		Username:      username,
	})
	if err != nil {
		return nil, err
	}
	task := asynq.NewTask(UserUpdateEmailVerificaitonV1, payload, nil)
	return task, nil
}

func EnqueueEmailVerificationCodeTask(ctx context.Context, asynqClient *asynq.Client, requestTime, ip, location, device, year, receiverEmail, username string) error {
	taskPayload, err := NewEmailVerificationPayload(requestTime, ip, location, device, year, receiverEmail, username)
	if err != nil {
		return err
	}
	info, err := asynqClient.EnqueueContext(ctx, taskPayload,
		asynq.Queue("email"),
		asynq.MaxRetry(1),
		asynq.Timeout(60*time.Second),
		asynq.Retention(1*time.Hour), // how long to keep the task in the queue
		// asynq.ProcessIn(1*time.Second), // how long to wait before processing the task
	)
	if err != nil {
		return err
	}
	zap.S().Infow("Enqueued update pending orders task", "task_id", info.ID, "queue", info.Queue, "max_retry", info.MaxRetry, "timeout", info.Timeout, "receiverEmail", receiverEmail)
	return nil
}

func HandleEmailVerificationCodeSendOp(ctx context.Context, t *asynq.Task, redisClient *redis.Client, mailDialer *gomail.Dialer, authGmailCache *cache.AuthGmailCache) error {
	var emailVerificationPayload EmailVerificationPayload
	if err := json.Unmarshal(t.Payload(), &emailVerificationPayload); err != nil {
		return err
	}
	zap.S().Infow("Handling email verification code send operation", "payload", emailVerificationPayload)
	tmpl, err := mail.SetupAuthVerificationMailTemplate()
	if err != nil {
		zap.S().Error("failed to setup auth verification mail template", zap.Error(err))
		return err
	}

	mailClient := mail.NewMailClient(tmpl)
	randomCode := services.GenerateRandomString(EMAIL_VERIFICATION_CODE_LENGTH)

	if err := mailClient.SendAuthVerificationCode(
		*mailDialer,
		emailVerificationPayload.ReceiverEmail,
		randomCode,
		EMAIL_VERIFICATION_CODE_LIFETIME_DURATION.String(),
		emailVerificationPayload.RequestTime,
		emailVerificationPayload.IP,
		emailVerificationPayload.Location,
		emailVerificationPayload.Device,
		emailVerificationPayload.Year,
	); err != nil {
		return err
	}

	if err := authGmailCache.SetGmailVerificationCode(
		ctx,
		redisClient,
		EMAIL_VERIFICATION_CODE_LIFETIME_DURATION,
		emailVerificationPayload.ReceiverEmail,
		emailVerificationPayload.Username,
		randomCode,
	); err != nil {
		zap.S().Error("failed to set gmail verification code", zap.Error(err))
		return err
	}

	return nil
}
