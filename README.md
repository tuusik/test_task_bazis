# Асинхронный сервис обработки платежей

Сервис принимает платежи по HTTP, сохраняет платеж и событие в PostgreSQL,
публикует событие в RabbitMQ через transactional outbox, асинхронно эмулирует
процессинг и отправляет результат на webhook клиента.

## Как проходит платеж

1. API проверяет `X-API-Key` и обязательный `Idempotency-Key`.
2. Платеж со статусом `pending` и событие `payment.created` записываются одной
   транзакцией PostgreSQL.
3. Outbox worker через `FOR UPDATE SKIP LOCKED` берет claim ровно на одно событие
   непосредственно перед публикацией и освобождает DB-транзакцию до обращения
   к RabbitMQ. После publisher confirm событие отмечается опубликованным, а
   ошибка сохраняется и планируется для отдельной повторной попытки. Размер
   batch ограничивает число таких последовательных операций за один проход.
4. Consumer ждет случайное время от 2 до 5 секунд и выбирает результат с
   вероятностью 90% `succeeded` / 10% `failed`.
5. Финальный статус сохраняется в БД до HTTP-вызова, после чего consumer
   получает ограниченный по времени claim и отправляет webhook.
6. При технической ошибке сообщение повторяется до трех попыток с задержками
   1 и 2 секунды. После третьей ошибки RabbitMQ направляет его через
   `payments.dlx` в `payments.dlq`.

Статус платежа не пересчитывается при повторной попытке webhook. Поле
`webhook_sent_at` фиксируется только после успешного ответа `2xx`.

## Быстрый запуск

Нужны Docker и Docker Compose.

```bash
cp .env.example .env
docker compose up --build
```

После запуска доступны:

- API и Swagger: `http://localhost:8000/docs`;
- RabbitMQ Management: `http://localhost:15672`;
- PostgreSQL: `localhost:5432`.

Локальные учетные данные RabbitMQ из `.env.example`: `payments` / `payments`.
Значение `API_KEY=local-development-api-key` предназначено только для локальной
разработки — для другого окружения его нужно заменить.

Миграции выполняются отдельным контейнером `migrate` до старта API и workers.

## API

### Создать платеж

```bash
curl -i -X POST http://localhost:8000/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: local-development-api-key' \
  -H 'Idempotency-Key: order-42-attempt-1' \
  -d '{
    "amount": "1250.50",
    "currency": "RUB",
    "description": "Оплата заказа №42",
    "metadata": {"order_id": "42"},
    "webhook_url": "https://example.com/webhooks/payments"
  }'
```

Ответ `202 Accepted`:

```json
{
  "payment_id": "7fe2d85c-68b9-4e03-9840-0ed2d1444011",
  "status": "pending",
  "created_at": "2026-08-01T12:00:00Z"
}
```

Повторный запрос с тем же `Idempotency-Key` и тем же телом вернет тот же платеж
и не создаст второе outbox-событие. Если тело отличается, API вернет
`409 Conflict`.

### Получить платеж

```bash
curl http://localhost:8000/api/v1/payments/7fe2d85c-68b9-4e03-9840-0ed2d1444011 \
  -H 'X-API-Key: local-development-api-key'
```

Отсутствующий или неверный API-ключ дает `401 Unauthorized`, неизвестный платеж
— `404 Not Found`.

## Формат webhook

Webhook получает `POST` с JSON:

```json
{
  "payment_id": "7fe2d85c-68b9-4e03-9840-0ed2d1444011",
  "status": "succeeded",
  "amount": "1250.50",
  "currency": "RUB",
  "processed_at": "2026-08-01T12:00:03Z",
  "metadata": {"order_id": "42"}
}
```

Заголовок `X-Webhook-Id` равен `payment_id` и остается одинаковым при повторных
попытках. Получатель может использовать его как ключ дедупликации. Доставка
имеет семантику **at least once**: если процесс упадет после успешного HTTP-ответа,
но до сохранения `webhook_sent_at`, webhook может прийти повторно.

Сетевой сбой, ответы `408`, `425`, `429` и `5xx` считаются временными ошибками и
запускают retry сообщения. Остальные ответы вне диапазона `2xx` считаются
постоянными ошибками и направляются сразу в DLQ.

По умолчанию webhook не может указывать на loopback, private, link-local или
другой непубличный IP-адрес. Для изолированной локальной разработки эту защиту
можно явно отключить через `WEBHOOK_ALLOW_PRIVATE_NETWORKS=true`; в общедоступном
окружении отключать ее не следует.

## Локальная разработка

```bash
python3.13 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env
alembic upgrade head
```

Компоненты можно запустить в отдельных терминалах:

```bash
uvicorn app.main:app --reload
faststream run app.workers.consumer:app
python -m app.workers.outbox
```

Тесты с проверкой покрытия:

```bash
pytest
```

Интеграционные тесты используют запущенные PostgreSQL и RabbitMQ из
`docker-compose` и создают изолированную временную схему/очереди:

```bash
RUN_INTEGRATION_TESTS=1 pytest -m integration --no-cov
```

Проверка стиля и форматирования:

```bash
ruff check .
ruff format --check .
```

Чистый архив для отправки создается командой:

```bash
make package
```

Результат сохраняется в `dist/test_task_bazis.zip`. В архив не попадают `.venv`,
`.env`, IDE-файлы, coverage, кэши, байткод и временные каталоги.

Остановить контейнеры:

```bash
docker compose down
```

Удалить контейнеры вместе с данными PostgreSQL и RabbitMQ:

```bash
docker compose down -v
```

## Гарантии и ограничения

- HTTP-идемпотентность обеспечивается уникальным `Idempotency-Key`.
- Атомарность записи платежа и события обеспечивает transactional outbox.
- Публикация outbox и доставка RabbitMQ имеют семантику at least once;
  `message_id` равен идентификатору outbox-события.
- Claim/lease не позволяет двум workers одновременно публиковать одно outbox-
  событие или отправлять один webhook. TTL каждого claim больше сетевого
  timeout и проверяется при загрузке конфигурации.
- Уже успешно доставленный webhook повторно не отправляется. При падении между
  HTTP-ответом и записью `webhook_sent_at` остается допустим редкий дубликат,
  поэтому получатель должен дедуплицировать `X-Webhook-Id`.
- DLQ сохраняет сообщения, которые не удалось обработать за три попытки, для
  ручной диагностики и последующего replay.
