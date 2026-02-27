package tasks

// Task patterns follow this rule: <bounded-context>.<entity>.<action>.v<version> -> like: wallet.transaction.send.v1

const (
	EvmTransactionUpdateStatusV1 = "transaction.evmtransaction.update.v1"

	UserUpdateEmailVerificaitonV1 = "user.email.update.v1"

	TransferEventCreateV1 = "transfer_event.create.v1"
)
