package configs

import (
	"fmt"
	"strings"

	"log"

	"github.com/ilyakaznacheev/cleanenv"
	"go.uber.org/multierr"
	"go.uber.org/zap/zapcore"
)

var C Configuration

func LoadConfig(envFilePath string) error {
	cfg := &Configuration{}

	if errConfig := cleanenv.ReadConfig(envFilePath, cfg); errConfig != nil {
		if errEnv := cleanenv.ReadEnv(cfg); errEnv != nil {
			log.Fatal(multierr.Combine(errConfig, errEnv))
		}
	}

	cfg.PrepareConfig()
	C = *cfg

	return nil
}

type Configuration struct {
	App struct {
		Env   string `env:"APP_ENV" env-default:"DEVELOPMENT"` // DEVELOPMENT, PRODUCTION
		Debug bool   `env:"APP_DEBUG" env-default:"True"`

		Name string `env:"APP_NAME" env-default:"coinhub-service"`
		Host string `env:"APP_HOST" env-default:"0.0.0.0"`
		Port string `env:"APP_PORT" env-default:"8001"`

		GracePeriod int    `env:"APP_GRACE_PERIOD" env-default:"1000"`
		Address     string `env:"APP_ADDRESS" env-default:"localhost:8001"`

		InstType string `env:"APP_INST_TYPE" env-required:"true"` // API, WORKER
		Version  string `env:"APP_VERSION" env-required:"true"`
		TimeZone string `env:"APP_TIMEZONE" env-required:"true"`

		CORSAllow string `env:"APP_CORS_ALLOW" env-default:"http://localhost:8001"`

		UnderMaintenance bool `env:"APP_UNDER_MAINTENANCE" env-default:"false"`

		JWTSecret     string `env:"APP_JWT_SECRET" env-default:"thisisbestsecretintheworldsomething"`
		SessionSecret string `env:"APP_SESSION_SECRET" env-default:"thisisbestsecretintheworldbrother"`

		// TODO(feature): this network setup just supports two kind of network simultaneuosly, you can setup it better.
		NetworkStatus           string `env:"NETWORK_STATUS" env-default:"TESTNET"`
		HDWalletMnemonic        string `env:"HDWALLET_MNEMONIC" env-required:"true"`
		ETHClientTestnet        string `env:"ETH_CLIENT_TESTNET" env-required:"true"`
		ETHClientMainnet        string `env:"ETH_CLIENT_MAINNET" env-required:"true"`
		WSClientEthereumMainnet string `env:"WS_CLIENT_ETHEREUM_MAINNET" env-required:"true"`
		WSClientEthereumTestnet string `env:"WS_CLIENT_ETHEREUM_TESTNET" env-required:"true"`

		IPInfoServiceToken string `env:"IPINFO_SERVICE_TOKEN" env-default:"f7469fe42c3c6c"`
	}

	MessageBroker struct {
		MessageStreamerHost string `env:"MESSAGE_STREAMER_HOST" env-default:"localhost"`
		MessageStreamerPort string `env:"MESSAGE_STREAMER_PORT" env-default:"9092"`
	}

	Lang struct {
		Locale         string `env:"LANG_LOCALE" env-default:"en"`
		FallbackLocale string `env:"LANG_FALLBACK" env-default:"en"`
	}

	Observe struct {
		// log levels : info, warn, error, debug, fatal, panic or trace
		LogLevel  zapcore.Level `env:"OBSERVE_LOG_LEVEL" env-default:"debug"`
		SentryDSN string        `env:"OBSERVE_SENTRY_DSN"`
	}

	Storage struct {
		DatabaseUrl string `env:"DATABASE_URL" env-required:"true"`

		Redis struct {
			Host     string `env:"STORAGE_REDIS_HOST" env-required:"true"`
			Port     int    `env:"STORAGE_REDIS_PORT" env-required:"true"`
			Username string `env:"STORAGE_REDIS_USERNAME" env-required:"true"`
			Password string `env:"STORAGE_REDIS_PASSWORD" env-required:"true"`
		}
	}

	Service struct {
		QueueDB int `env:"SERVICE_QUEUE_DB" env-default:"7"`
		CacheDB int `env:"SERVICE_CACHE_DB" env-default:"4"`
	}

	Mail struct {
		SMTPHost     string `env:"MAIL_SMTP_HOST" default:"smtp.gmail.com"`
		SMTPPort     int    `env:"MAIL_SMTP_PORT" default:"587"`
		SMTPUsername string `env:"MAIL_SMTP_USERNAME" env-required:"true"`
		SMTPPassword string `env:"MAIL_SMTP_PASSWORD" env-required:"true"`
		FromEmail    string `env:"MAIL_FROM_EMAIL" env-required:"true"`
		FromName     string `env:"MAIL_FROM_NAME" default:"Coinhub"`
	}

	Market struct {
		ExternalPriceFeed struct {
			ProviderName    string `env:"MARKET_PRICEFEED_PROVIDER" env-default:""`
			BaseURL         string `env:"MARKET_PRICEFEED_BASE_URL" env-default:""`
			PriceFeedAPIKey string `env:"MARKET_PRICE_FEED_API_KEY" env-required:"true"`
		}
	}

	// TODO(security) : Add the allowed origins here
	AllowedOrigins struct {
		FrontendApplication string `env:"ALLOWED_ORIGINS_FRONTEND_APPLICATION" env-default:"https://example.com"`
	}

	ServiceURLAPI string `env:"SERVICE_URL_API" env-default:"https://local.host"`
}

func (c *Configuration) PrepareConfig() {
	c.App.Env = strings.ToUpper(c.App.Env)
}

func (c *Configuration) RedisAddress() string {
	return fmt.Sprintf("%s:%d", c.Storage.Redis.Host, c.Storage.Redis.Port)
}
