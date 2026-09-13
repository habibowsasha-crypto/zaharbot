# FIX REPORT - Reaction repost source filter + full on/off switch

Добавлено/проверено:

- `REACTION_REPOST_ENABLED` - главный переключатель функции реакционного дубля.
  - `true` = функция работает.
  - `false` = бот полностью игнорирует реакции.

- `REACTION_REPOST_SOURCE_CHATS` - список разрешённых источников для реакции.
  - Если указаны источники, бот срабатывает только в них.
  - Если реакция поставлена в другом канале/чате, бот игнорирует событие.

Рабочий пример:

```env
REACTION_REPOST_ENABLED=true
REACTION_REPOST_SOURCE_CHATS=-1001111111111,-1002222222222
REACTION_REPOST_TARGET_CHAT=-1003333333333
REACTION_REPOST_EMOJIS=🔥,☠️,👍
REACTION_REPOST_ALLOWED_USER_IDS=6835564228
REACTION_REPOST_DEBUG=false
```

Важно:

- Старая функция `SOURCE_CHATS -> TARGET_CHAT/темы` не ломается.
- `ROUTE_MAP` остаётся дополнительным маршрутом.
- Реакционный дубль работает через событие Telethon `events.Raw`, без цикла и без постоянного опроса Telegram.
