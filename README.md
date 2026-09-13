# Telegram Userbot Reposter - стабильная версия

Python/Telethon userbot для копирования новых постов из Telegram-каналов, доступных личному аккаунту, в выбранный канал / группу.

> Важно: используй только там, где у тебя есть право копировать и публиковать контент. `SESSION_STRING` равен доступу к Telegram-аккаунту. Не заливай его в GitHub и не отправляй никому.

---

## Что умеет

- Работает через личный Telegram-аккаунт, а не Bot API.
- Слушает один или несколько каналов-источников.
- Копирует новые посты в целевой канал / группу.
- Поддерживает текст, фото, видео, документы, подписи к медиа.
- Поддерживает альбомы / grouped media.
- Режим `copy` - публикует как новый пост.
- Режим `forward` - пересылает оригинальное сообщение.
- Сохраняет порядок публикаций через единую очередь.
- Делает глобальную задержку между отправками.
- Защищает от дублей в памяти и через SQLite.
- Проверяет доступность `SOURCE_CHATS`, `TARGET_CHAT`, `LOG_CHAT` на старте.
- Блокирует опасную конфигурацию, когда `TARGET_CHAT` случайно указан в `SOURCE_CHATS`.
- Корректно обрабатывает web preview у ссылок как текст, а не как файл.
- Не зависает на Railway при плохой `SESSION_STRING`, а сразу показывает понятную ошибку.
- Может отправлять в Telegram topics через `TARGET_THREAD_ID` в режиме `copy`.
- Поддерживает маршрутизацию обычных источников по темам: `SOURCE_CHATS=-100source:thread`.
- Поддерживает маршрутизацию из темы в тему: `TOPIC_ROUTE_MAP=source_chat:source_thread>target_chat:target_thread`.
- Пишет логи в консоль и опционально в `LOG_CHAT`.
- Готов под Railway + GitHub.

---

## Что не надо ожидать

Это не магический клонер Telegram один в один. Ограничения Telegram и Telethon остаются:

- protected content может не копироваться;
- опросы, dice, контакты, геолокации и нестандартные вложения могут не копироваться как оригинал;
- callback-кнопки чужих ботов не всегда можно пересоздать;
- изменения и удаления старых постов не синхронизируются, бот работает только с новыми сообщениями;
- `TARGET_THREAD_ID`, `SOURCE_TOPIC_MAP` и `TOPIC_ROUTE_MAP` надежнее использовать с `COPY_MODE=copy`, а не с `forward`.

---

## Структура проекта

```txt
telegram-userbot-reposter/
├── main.py              # Основной userbot
├── generate_session.py  # Генератор SESSION_STRING
├── list_chats.py        # Помощник для получения chat_id
├── requirements.txt     # Зависимости
├── .env.example         # Пример ENV
├── .gitignore           # Что нельзя заливать
├── Procfile             # Railway worker
├── railway.json         # Railway start command
└── README.md            # Инструкция
```

---

## 1. Получить API_ID и API_HASH

1. Открой `my.telegram.org`.
2. Войди по номеру телефона.
3. Перейди в `API development tools`.
4. Создай приложение.
5. Сохрани:
   - `api_id`
   - `api_hash`

---

## 2. Локальный запуск

Создать виртуальное окружение:

```bash
python -m venv venv
```

Windows:

```bash
venv\Scripts\activate
```

macOS/Linux:

```bash
source venv/bin/activate
```

Установить зависимости:

```bash
pip install -r requirements.txt
```

---

## 3. Создать `.env`

Скопируй пример:

Windows CMD:

```cmd
copy .env.example .env
```

PowerShell:

```powershell
Copy-Item .env.example .env
```

macOS/Linux:

```bash
cp .env.example .env
```

Минимальный набор:

```env
API_ID=12345678
API_HASH=your_api_hash
SESSION_STRING=your_string_session
SOURCE_CHATS=-1001111111111
TARGET_CHAT=-1003333333333
COPY_MODE=copy
DELAY_SECONDS=1
ENABLE_ALBUMS=true
ENABLE_DUPLICATE_PROTECTION=true
```

Полный пример лежит в `.env.example`.

---

## 4. Сгенерировать SESSION_STRING

Запусти локально:

```bash
python generate_session.py
```

Скрипт попросит:

- номер телефона;
- код из Telegram;
- 2FA пароль, если включен.

После этого он выведет `SESSION_STRING`.

Скопируй его в `.env` локально или в Railway Variables.

Никогда не публикуй `SESSION_STRING`.

---

## 5. Узнать chat_id каналов

После заполнения `.env` запусти:

```bash
python list_chats.py
```

Пример вывода:

```txt
ID: -1001111111111 | title: Source Channel | username: @-
ID: -1003333333333 | title: My Target Channel | username: @my_channel
```

Эти ID используй в `.env`:

```env
SOURCE_CHATS=-1001111111111,-1002222222222
TARGET_CHAT=-1003333333333
# Для темы источника -> тему получателя:
# TOPIC_ROUTE_MAP=-1001111111111:4>-1003333333333:10
```

Можно использовать `@username`, если канал публичный.

---

## 6. Маршрутизация по темам

### Обычный канал → тема в целевой группе

Если все источники отправляются в одну целевую forum-группу, но каждый источник должен идти в свою тему:

```env
SOURCE_CHATS=-1001111111111:4,-1002222222222:10
TARGET_CHAT=-1003333333333
```

Расшифровка:

```txt
-1001111111111 -> тема 4 внутри TARGET_CHAT
-1002222222222 -> тема 10 внутри TARGET_CHAT
```

### Тема источника → тема получателя

Если источник сам является forum-группой с темами и нужно брать посты только из конкретных тем:

```env
TOPIC_ROUTE_MAP=-1001111111111:4>-1003333333333:10,-1001111111111:8>-1003333333333:12
```

Расшифровка:

```txt
из группы -1001111111111, темы 4 -> в группу -1003333333333, тему 10
из группы -1001111111111, темы 8 -> в группу -1003333333333, тему 12
```

Эта логика работает вместе со старой. Можно одновременно использовать:

```env
SOURCE_CHATS=-1004444444444:6,-1005555555555:7
TARGET_CHAT=-1003333333333
TOPIC_ROUTE_MAP=-1001111111111:4>-1003333333333:10
```

Приоритет такой:

```txt
1. Если сообщение пришло из пары source_chat + source_thread из TOPIC_ROUTE_MAP -> отправляем по TOPIC_ROUTE_MAP.
2. Если TOPIC_ROUTE_MAP не подходит -> используем SOURCE_CHATS / SOURCE_TOPIC_MAP / TARGET_THREAD_ID.
3. Если source_chat есть в TOPIC_ROUTE_MAP, но тема не указана в маршруте -> сообщение игнорируется.
```

---

## 7. Запуск локально

```bash
python main.py
```

Нормальный запуск выглядит примерно так:

```txt
[INFO] Userbot started
[INFO] Connected as: username_or_id
[INFO] SOURCE_CHATS: [...]
[INFO] TARGET_CHAT: ...
[INFO] Preflight check started
[INFO] SOURCE OK: ...
[INFO] TARGET OK: ...
[INFO] Preflight check passed
[INFO] Queue worker started
[INFO] Userbot is running. Waiting for new posts...
```

---

## 8. Railway Variables

В Railway добавь Variables:

```env
API_ID=12345678
API_HASH=your_api_hash
SESSION_STRING=your_string_session
SOURCE_CHATS=-1001111111111,-1002222222222
TARGET_CHAT=-1003333333333
# Для темы источника -> тему получателя:
# TOPIC_ROUTE_MAP=-1001111111111:4>-1003333333333:10
TARGET_THREAD_ID=
LOG_CHAT=
COPY_MODE=copy
DELAY_SECONDS=1
ENABLE_ALBUMS=true
ENABLE_DUPLICATE_PROTECTION=true
PERSIST_PROCESSED=true
PROCESSED_DB_PATH=processed_messages.sqlite3
QUEUE_MAXSIZE=1000
LINK_PREVIEW=true
PRESERVE_BUTTONS=false
MAX_FLOOD_WAIT_SECONDS=900
LOG_LEVEL=INFO
```

Обязательные:

- `API_ID`
- `API_HASH`
- `SESSION_STRING`
- `SOURCE_CHATS`
- `TARGET_CHAT`

Необязательные:

- `TARGET_THREAD_ID`
- `LOG_CHAT`
- `COPY_MODE`
- `DELAY_SECONDS`
- `ENABLE_ALBUMS`
- `ENABLE_DUPLICATE_PROTECTION`
- `PERSIST_PROCESSED`
- `PROCESSED_DB_PATH`
- `QUEUE_MAXSIZE`
- `LINK_PREVIEW`
- `PRESERVE_BUTTONS`
- `MAX_FLOOD_WAIT_SECONDS`
- `LOG_LEVEL`

---

## 8. Railway Deploy

1. Создай GitHub репозиторий.
2. Загрузи туда файлы проекта.
3. Не загружай `.env`.
4. В Railway создай `New Project`.
5. Выбери `Deploy from GitHub repo`.
6. Открой `Variables` и добавь ENV.
7. Start Command:

```bash
python main.py
```

В проекте уже есть `railway.json`, где указан этот start command.

---

## 9. Настройки

### `COPY_MODE`

```env
COPY_MODE=copy
```

Пост публикуется как новый. Обычно это лучший вариант для закрытых каналов.

```env
COPY_MODE=forward
```

Пост пересылается как forward. Может показывать источник, если Telegram это позволяет.

---

### `DELAY_SECONDS`

```env
DELAY_SECONDS=1
```

Глобальная задержка между отправками. Теперь она работает через очередь, поэтому не ломается при потоке сообщений.

---

### `TARGET_THREAD_ID`

Для отправки в Telegram topic / forum-тему:

```env
TARGET_THREAD_ID=123
```

Обычно это ID первого сообщения темы. Если не используешь темы, оставь пустым.

Лучше использовать вместе с:

```env
COPY_MODE=copy
```

---

### `PERSIST_PROCESSED`

```env
PERSIST_PROCESSED=true
```

Бот сохраняет обработанные сообщения в SQLite, чтобы уменьшить риск дублей после рестарта.

Файл базы:

```env
PROCESSED_DB_PATH=processed_messages.sqlite3
```

---

### `PRESERVE_BUTTONS`

```env
PRESERVE_BUTTONS=false
```

По умолчанию выключено. Это честно безопаснее, потому что чужие callback-кнопки не всегда можно пересоздать.

Можно попробовать:

```env
PRESERVE_BUTTONS=true
```

Но если начнутся ошибки на постах с кнопками - верни `false`.

---

## 10. Частые ошибки

### `Cannot access SOURCE_CHAT`

Аккаунт не видит канал-источник или ID указан неверно.

Решение:

- проверь, что личный аккаунт подписан на источник;
- запусти `python list_chats.py`;
- скопируй правильный ID.

---

### `Cannot access TARGET_CHAT`

Аккаунт не видит целевой канал / группу или не имеет доступа.

Решение:

- добавь аккаунт в целевой канал;
- дай право публиковать, если это канал;
- проверь ID через `list_chats.py`.

---

### `TARGET_CHAT is also present in SOURCE_CHATS`

Ты случайно добавил целевой канал в источники. Так можно создать бесконечный цикл репостов.

Решение:

- убери `TARGET_CHAT` из `SOURCE_CHATS`.

---

### `SESSION_STRING is invalid or corrupted`

Сессия неправильная или повреждена.

Решение:

```bash
python generate_session.py
```

И заново вставь полученную строку в Variables.

---

## 11. Что было усилено в этой версии

- Добавлена единая очередь обработки.
- Исправлен риск нарушения порядка сообщений.
- Исправлена проблема с web preview у ссылок.
- Добавлена persistent-защита от дублей через SQLite.
- Добавлена preflight-проверка чатов на старте.
- Добавлена защита от бесконечного цикла репостов.
- Добавлена поддержка `TARGET_THREAD_ID` для topics в copy-режиме.
- Улучшены понятные ошибки по ENV и SESSION_STRING.
- Убраны служебные `__pycache__` из готового архива.

