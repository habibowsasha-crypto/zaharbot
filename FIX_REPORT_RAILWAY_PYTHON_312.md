# FIX: Railway Python 3.12

Проблема: Railway пытался собрать проект на Python 3.13.14, но сборка падала с ошибкой `no precompiled python found`.

Что добавлено:

- `.python-version` = `3.12.8`
- `runtime.txt` = `python-3.12.8`
- `nixpacks.toml` с `python312`

Цель: принудительно зафиксировать Python 3.12 для Railway/Nixpacks и убрать попытку сборки на Python 3.13.

После загрузки архива в GitHub нужно сделать Commit и Redeploy latest commit в Railway.
