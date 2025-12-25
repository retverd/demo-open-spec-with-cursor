# Demo OpenSpec with Cursor

Демонстрация генерации кода при помощи [OpenSpec](https://github.com/Fission-AI/OpenSpec) через Cursor Pro

## Сбор курса USD/RUB (ЦБ РФ) → Parquet

Изменение `add-cbr-usdrub-parquet-export` добавляет CLI для получения курса USD/RUB за последние 7 дней (включая сегодня) и сохранения в Parquet в текущую папку.

### Запуск

```bash
python cbr_usdrub_fetcher.py
# или с указанием папки
python cbr_usdrub_fetcher.py --out-dir .
```

Будет создан файл вида `cbr_usdrub_YYYY-MM-DD_HHMMSS.parquet` в выбранной папке (по умолчанию — текущая).

### Тесты

- Юнит-тесты: `pytest -m "not integration"`
- Интеграционные тесты (реальный вызов ЦБ РФ): `PYTEST_RUN_INTEGRATION=1 pytest -m integration`

> Примечание: установка зависимостей может требовать бинарные колёса для pandas/pyarrow; для запуска тестов и CLI используйте подготовленное окружение/venv.
