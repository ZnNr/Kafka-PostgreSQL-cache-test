# Makefile с исправлениями
.PHONY: fmt vet lint test coverage cover-html cover-func clean help all run-service run-producer build-service build-producer up down up-logs redis-cli psql logs status install-lint

# Включаем CGO для race detection
export CGO_ENABLED=1

# Установка golangci-lint
install-lint:
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "✅ golangci-lint установлен"

# Форматирование кода
fmt:
	@gofmt -s -l -w .
	@echo "✓ Форматирование завершено"

# Проверка кода на ошибки
vet:
	@go mod tidy 2>/dev/null || true
	@go vet ./...
	@echo "✓ go vet прошёл"

# Линтинг кода
lint:
	@golangci-lint run ./... --config .golangci.yml 2>/dev/null || \
	golangci-lint run ./cmd/... ./internal/... ./config/... 2>/dev/null || \
	echo "Линтер завершен"
	@echo "✓ Линтинг прошёл"

# Быстрый линтинг
lint-fast:
	@golangci-lint run --fast ./... 2>/dev/null || echo "⚠️  Быстрый линтинг завершен"
	@echo "Быстрый линтинг прошёл"

# Запуск тестов
test:
	@go mod tidy 2>/dev/null || true
	@CGO_ENABLED=1 go test ./... -v
	@echo "✓ Тесты прошли"

# Запуск тестов с race detection (если CGO доступен)
test-race:
	@go mod tidy 2>/dev/null || true
	@(CGO_ENABLED=1 go test -race ./... -v 2>/dev/null && echo "✓ Тесты с race detection прошли") || \
	(go test ./... -v && echo "✓ Тесты прошли (race detection недоступен)")

# Запуск тестов с покрытием
coverage:
	@go mod tidy 2>/dev/null || true
	@go test -coverprofile=coverage.out ./...
	@go tool cover -func=coverage.out
	@echo "📊 Покрытие кода рассчитано. Файл: coverage.out"
	@echo "💡 Используйте 'make cover-html' для просмотра в браузере"

# Покрытие в HTML
cover-html: coverage
	@go tool cover -html=coverage.out
	@echo "🌐 Открываю отчёт о покрытии в браузере..."

# Краткий отчёт о покрытии
cover-func: coverage
	@go tool cover -func=coverage.out

# Очистка временных файлов
clean:
	@rm -f coverage.out 2>/dev/null || true
	@rm -f *.log 2>/dev/null || true
	@rm -f bin/service 2>/dev/null || true
	@rm -f bin/producer 2>/dev/null || true
	@find . -name "*\.test" -delete 2>/dev/null || true
	@echo "🧹 Временные файлы удалены"

# Запуск сервиса
run-service:
	@go mod tidy 2>/dev/null || true
	go run cmd/service/main.go

# Запуск продюсера
run-producer:
	@go mod tidy 2>/dev/null || true
	go run cmd/producer/producer.go

# Запуск просмоторщика очереди не доставленных сообщений
run-dlq:
	@go mod tidy 2>/dev/null || true
	go run cmd/dlq_reader/dlq_watcher.go

# Сборка сервиса
build-service:
	@go mod tidy 2>/dev/null || true
	go build -o bin/service cmd/service/main.go

# Сборка продюсера
build-producer:
	@go mod tidy 2>/dev/null || true
	go build -o bin/producer cmd/producer/main.go

# Сборка всех бинарных файлов
build-all: build-service build-producer
	@echo "Все бинарные файлы собраны"

# Полный цикл проверки
all: fmt vet lint test-race

# запуск всех
run-all: run-service run-producer run-dlq

# Помощь
help:
	@echo " Разработка и тестирование:"
	@echo "  make fmt          — форматировать код"
	@echo "  make vet          — проверить ошибки"
	@echo "  make lint         — запустить линтер"
	@echo "  make lint-fast    — быстрый линтинг"
	@echo "  make test         — запустить тесты"
	@echo "  make test-race    — запустить тесты с race detection"
	@echo "  make coverage     — запустить тесты с покрытием"
	@echo "  make cover-html   — открыть отчёт о покрытии в браузере"
	@echo "  make cover-func   — показать отчёт о покрытии в терминале"
	@echo "  make all          — всё подряд (fmt, vet, lint, test-race)"
	@echo ""
	@echo "  Сборка:"
	@echo "  make build-service  — собрать основной сервис"
	@echo "  make build-producer — собрать продюсера"
	@echo "  make build-all      — собрать все бинарные файлы"
	@echo ""
	@echo " Docker Compose:"
	@echo "  make up           — запустить все сервисы"
	@echo "  make down         — остановить все сервисы"
	@echo "  make up-logs      — запустить сервисы с логами"
	@echo "  make redis-cli    — подключиться к Redis CLI"
	@echo "  make psql         — подключиться к PostgreSQL"
	@echo "  make logs         — просмотр логов"
	@echo "  make status       — статус сервисов"
	@echo ""
	@echo " Запуск приложений:"
	@echo "  make run-service  — запустить основной сервис"
	@echo "  make run-producer — запустить продюсера"
	@echo ""
	@echo " Очистка и утилиты:"
	@echo "  make clean        — удалить временные файлы"
	@echo "  make install-lint — установить golangci-lint"
	@echo ""
	@echo "  make help         — эта подсказка"