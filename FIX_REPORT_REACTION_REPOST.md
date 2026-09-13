# FIX REPORT - Reaction Repost + separate account mode

Добавлено:

- `REACTION_REPOST_ENABLED` - включает ручное дублирование старых постов по реакции.
- `REACTION_REPOST_TARGET_CHAT` - куда отправлять пост после реакции.
- `REACTION_REPOST_TARGET_THREAD_ID` - тема назначения, если нужна.
- `REACTION_REPOST_EMOJIS` - какие реакции запускают дубль.
- `REACTION_REPOST_ALLOWED_USER_IDS` - какие пользователи могут запускать дубль.
- `REACTION_REPOST_SOURCE_CHATS` - ограничение источников для реакции.
- `REACTION_REPOST_DEBUG` - логирование raw reaction update.

Также исправлено:

- `ROUTE_MAP` и `TOPIC_ROUTE_MAP` теперь добавляются в список прослушиваемых чатов автоматически.
- `SOURCE_CHATS` остаётся старым маршрутом и не ломается.
- `ROUTE_MAP` работает как дополнительный маршрут.

Формат:

```env
SOURCE_CHATS=-100source:12
TARGET_CHAT=-100pirate
ROUTE_MAP=-100source>-100extra
```

Результат:

- источник -> PIRATE INSIDE / тема 12
- источник -> extra target

Ручной дубль:

```env
REACTION_REPOST_ENABLED=true
REACTION_REPOST_TARGET_CHAT=-100extra
REACTION_REPOST_EMOJIS=🔥,☠️
REACTION_REPOST_ALLOWED_USER_IDS=6835564228
```

Поставил реакцию 🔥 или ☠️ на старый пост - бот копирует его в `REACTION_REPOST_TARGET_CHAT`.
