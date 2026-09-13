# Railway build fix

Исправление: добавлен Dockerfile на базе python:3.12-slim.
Это обходит ошибку Railway/mise при установке python через nixpacks/mise.

Что сделать:
1. Залить все файлы из архива в GitHub с заменой.
2. Commit changes.
3. Railway -> Redeploy latest commit.

Важно: .env/SESSION_STRING в GitHub не заливать.
