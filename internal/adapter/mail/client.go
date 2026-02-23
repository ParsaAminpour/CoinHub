package mail

import (
	_ "embed"
	"fmt"
	"html/template"

	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"gopkg.in/gomail.v2"
)

//go:embed template.html
var emailTemplate string

type MailClient struct {
	template *template.Template
}

type AuthVerificationMailData struct {
	Code          string
	ExpiryMinute  string
	RequestTime   string
	IP            string
	Location      string
	Device        string
	Year          string
	ReceiverEmail string
}

func NewMailClient(tmpl *template.Template) MailClient {
	return MailClient{template: tmpl}
}

func SetupAuthVerificationMailTemplate() (*template.Template, error) {
	tmpl, err := template.New("verification").Parse(emailTemplate)
	if err != nil {
		return nil, err
	}
	return tmpl, nil
}

func (mc MailClient) SendAuthVerificationCode(mailClient gomail.Dialer, toMail, verificationCode, expiryMinute, requestTime, ip, location, device, year string) error {
	data := AuthVerificationMailData{
		Code:          verificationCode,
		ExpiryMinute:  expiryMinute,
		RequestTime:   requestTime,
		IP:            ip,
		Location:      location,
		Device:        device,
		Year:          year,
		ReceiverEmail: toMail,
	}
	var body buffer.Buffer
	if err := mc.template.Execute(&body, data); err != nil {
		zap.S().Errorw("Failed to execute email template", "error", err)
		return fmt.Errorf("failed to render email template: %w", err)
	}

	m := gomail.NewMessage()

	fromEmail := mailClient.Username
	fromName := "Coinhub"
	m.SetHeader("From", m.FormatAddress(fromEmail, fromName))
	m.SetHeader("To", toMail)
	m.SetHeader("Subject", "Verify Your Email - Coinhub")
	m.SetBody("text/html", body.String())

	if err := mailClient.DialAndSend(m); err != nil {
		zap.S().Errorw("Failed to send email",
			"error", err,
			"to", toMail,
			"smtp_host", mailClient.Host,
		)
		return fmt.Errorf("failed to send email: %w", err)
	}

	zap.S().Infow("Verification email sent successfully", "to", toMail)

	return nil
}
