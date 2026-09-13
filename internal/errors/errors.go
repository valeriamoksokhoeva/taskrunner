package custom_errors

import "errors"
var ErrRateLimited = errors.New("перегружено: нет токена")
var ErrCtxCancelled = errors.New("отменено контекстом")
var ErrUnknownPriority = errors.New("неизвестный приоритет задачи, может быть только либо 'high', либо 'low')")
var ErrFullQueue = errors.New("очередь полная, мест нет")
var ErrWorkersLate = errors.New("воркеры не успели закрыться")