package entities

type RoleName string

const (
	RoleUser   RoleName = "user"
	RoleAdmin  RoleName = "admin"
	RoleSystem RoleName = "system"
)

type Role struct {
	ID   uint     `gorm:"primaryKey;autoIncrement"`
	Name RoleName `gorm:"type:varchar(50);uniqueIndex;not null"`
}

func (Role) TableName() string {
	return "roles"
}
