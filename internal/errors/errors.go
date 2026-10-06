package custom_errors

import "errors"

var (
	ErrRateLimited     = errors.New("перегружено: нет токена")
	ErrCtxCancelled    = errors.New("отменено контекстом")
	ErrUnknownPriority = errors.New("неизвестный приоритет: допустимо только high или low")
	ErrFullQueue       = errors.New("очередь полная, мест нет")
	ErrWorkersLate     = errors.New("воркеры не успели завершиться")
	ErrBadURL          = errors.New("некорректный url")
)
