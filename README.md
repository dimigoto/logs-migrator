# logs-migrator

---

## Установка

```bash
go build -o logs-migrator ./cmd/migrator
```

После сборки доступны две подкоманды:

```
logs-migrator export   — экспорт таблицы в архив
logs-migrator load     — импорт архива в новую таблицу
```

---

## Экспорт данных

### Пример

```bash
./logs-migrator export -dsn "username:password@tcp(127.0.0.1:3306)/db-name" -table log -pk id -columns "id,ins_ts,user_id,user_ip,customer_id,object,object_id,attribute,old_value,new_value,type,controller,action,log_extended_id,page_title,url" -workers 10 -chunk 1000000 -out ./export -max-exec-ms 120000
```

### Параметры `export`

| Флаг | Тип | Описание |
|------|-----|-----------|
| `-dsn` | string | DSN подключения к MySQL (пример: `user:pass@tcp(host:port)/dbname`) |
| `-table` | string | Имя таблицы для экспорта |
| `-pk` | string | Имя монотонно возрастающего PK (`INT`/`BIGINT`) |
| `-columns` | string | Список колонок через запятую (`*` — все) |
| `-where` | string | Дополнительный фильтр `WHERE` (без слова `WHERE`) |
| `-out` | string | Каталог для временных файлов (по умолчанию `./export`) |
| `-workers` | int | Количество параллельных воркеров |
| `-chunk` | int | Количество строк в одном CSV-файле |
| `-throttle-rows` | int | Лимит скорости (строк/сек на воркер, `0` = без ограничения) |
| `-max-exec-ms` | int | Лимит `MAX_EXECUTION_TIME` (в миллисекундах) |
| `-progress-inline` | bool | Выводить прогресс в одной строке (по умолчанию `true`) |

### Результат

- Каталог `./export` с файлами `table_01_000001.csv.gz`, `table_02_000002.csv.gz`, ...
- Сжатый архив `export.tar.gz` с полным дампом таблицы.
- После упаковки временные файлы автоматически удаляются.

---

## Импорт данных

### Пример

```bash
./logs-migrator load -dsn "username:password@tcp(127.0.0.1:3306)/db-name" -tar ./export.tar.gz -dst-table log -dst-columns "id,nid,ins_ts,user_id,user_ip,customer_id,object,object_id,attribute,old_value,new_value,type,controller,action,log_extended_id,page_title,url" -uuidv7-from-index 2 -uuidv7-tz America/Los_Angeles -workers 6
```

### Параметры `load`

| Флаг | Тип | Описание |
|------|-----|-----------|
| `-dsn` | string | DSN подключения к **новой базе** |
| `-tar` | string | Путь к архиву `export.tar.gz` |
| `-dst-table` | string | Таблица назначения |
| `-dst-columns` | string | Список колонок в таблице (первая должна быть `id`) |
| `-uuidv7-from-index` | int | 1-based индекс колонки даты для генерации UUIDv7 |
| `-uuidv7-tz` | string | Таймзона даты (`UTC`, `America/Los_Angeles`, и т. д.) |
| `-workers` | int | Количество потоков для импорта (по умолчанию = числу CPU) |

---

## Быстрый импорт (fast-load)

При запуске `load` утилита автоматически активирует **fast-load режим** для ускорения `LOAD DATA INFILE`:

```sql
SET GLOBAL unique_checks = 0;
SET GLOBAL foreign_key_checks = 0;
SET GLOBAL innodb_flush_log_at_trx_commit = 2;
SET GLOBAL sync_binlog = 0;
ALTER INSTANCE DISABLE INNODB REDO_LOG;
```

По завершении импортa настройки восстанавливаются:

```sql
ALTER INSTANCE ENABLE INNODB REDO_LOG;
SET GLOBAL innodb_flush_log_at_trx_commit = 1;
SET GLOBAL sync_binlog = 1;
SET GLOBAL unique_checks = 1;
SET GLOBAL foreign_key_checks = 1;
```

---

## Требования

- **Go** ≥ 1.22
- Параметр `secure_file_priv` должен быть включён и доступен для записи  
  (например, `/var/lib/mysql-files/`)

---

