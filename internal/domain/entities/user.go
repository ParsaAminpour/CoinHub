package entities

import (
	"time"

	"github.com/google/uuid"
	"gorm.io/gorm"
)

type Status string

const (
	StatusActive Status = "active"
	StatusBanned Status = "banned"
)

type GmailVerificationStatus string

const (
	GmailVerificationNotRegistered  GmailVerificationStatus = "not_registered"
	GmailVerificationStatusPending  GmailVerificationStatus = "pending"
	GmailVerificationStatusVerified GmailVerificationStatus = "verified"
)

type User struct {
	ID        uuid.UUID `gorm:"type:uuid;primaryKey;default:gen_random_uuid()"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	Username    *string `gorm:"unique;index;not null"`
	UserProfile Profile `gorm:"foreignKey:UserID"`

	Gmail                   *string                 `gorm:"unique;index"`
	GmailVerificationStatus GmailVerificationStatus `gorm:"type:varchar(20);default:'not_registered';not null"`
	GmailVerifiedAt         *time.Time              `gorm:"default:null"`

	Status        Status  `gorm:"type:varchar(20);default:'active';not null"`
	IsVerified    bool    `gorm:"default:false;not null"` // when the gmail becomes verified
	LocalTimezone *string `gorm:"default:'0';not null"`
}

func NewUser(firstname, lastname string, gmail string, gmailVerificationStatus GmailVerificationStatus, status Status) *User {
	user := &User{
		Gmail:                   &gmail,
		GmailVerificationStatus: gmailVerificationStatus,
		Status:                  status,
	}
	user.UserProfile = *NewProfile(firstname, lastname)
	return user
}

type Profile struct {
	ID        uint `gorm:"primaryKey"`
	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt gorm.DeletedAt `gorm:"index"`

	UserID    uuid.UUID `gorm:"type:uuid;uniqueIndex;not null"` // Foreign key to User
	Firstname string    `gorm:"size:100;not null"`              // cannot be empty, limited to 100 chars
	Lastname  string    `gorm:"size:100;not null"`              // cannot be empty, limited to 100 chars
	AvatarUrl string    `gorm:"size:255"`                       // optional, max length 255 chars
	Bio       string    `gorm:"size:500"`                       // optional, max length 500 chars
}

func NewProfile(firstname, lastname string) *Profile {
	return &Profile{
		Firstname: firstname,
		Lastname:  lastname,
	}
}
