# Logs Migrator

## Установка

```bash
# Клонировать репозиторий
git clone <repository-url>
cd logs-migrator

# Собрать для текущей платформы
go build -o logs-migrator ./cmd/migrator

# Пример сборки под конкретную OS и архитектуру
GOOS=linux GOARCH=amd64 go build -o logs-migrator ./cmd/migrator
```

## Использование

### Базовый пример

```bash
./logs-migrator \
  -src-dsn "user:password@tcp(source-host:3306)/source_db" \
  -dst-dsn "user:password@tcp(dest-host:3306)/dest_db" \
  -src-table "log" \
  -dst-table "log" \
  -chunk=1000000
```

### Пример с оптимизацией

```bash
./logs-migrator \
  -src-dsn "user:password@tcp(source-host:3306)/source_db" \
  -dst-dsn "user:password@tcp(dest-host:3306)/dest_db" \
  -src-table "log" \
  -dst-table "log" \
  -chunk=1000000 \
  -sw=4 \
  -lw=2 \
  -innodb-buffer-pool-gb=6 \
  -innodb-io-capacity=2000 \
  -innodb-io-capacity-max=4000 \
  -fast-load=true
```

### Пример с удаленным запуском (LOCAL INFILE)

```bash
./logs-migrator \
  -src-dsn "user:password@tcp(source-host:3306)/source_db" \
  -dst-dsn "user:password@tcp(dest-host:3306)/dest_db" \
  -local-infile \
  -chunk=1000000
```

### Пример с фильтрацией

```bash
./logs-migrator \
  -src-dsn "user:password@tcp(source-host:3306)/source_db" \
  -dst-dsn "user:password@tcp(dest-host:3306)/dest_db" \
  -src-filter "created_at >= '2024-01-01'" \
  -chunk=500000
```

## Параметры командной строки

### Обязательные параметры

| Параметр | Описание |
|----------|----------|
| `-src-dsn` | DSN для подключения к БД-источнику (формат: `user:password@tcp(host:port)/database`) |
| `-dst-dsn` | DSN для подключения к целевой БД (формат: `user:password@tcp(host:port)/database`) |

### Параметры источника данных

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `-src-table` | `log` | Имя таблицы-источника |
| `-src-nid` | `id` | Имя колонки с числовым ID в таблице-источнике |
| `-src-filter` | - | WHERE-фильтр для выборки данных (например: `id % 100 = 0`) |

### Параметры целевой БД

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `-dst-table` | `log` | Имя целевой таблицы |
| `-dst-nid` | `nid` | Имя колонки с числовым ID в целевой таблице |
| `-dst-uuid` | `id` | Имя колонки для UUID в целевой таблице |

### Параметры UUIDv7

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `-ts-idx` | `2` | Позиция колонки с timestamp в таблице-источнике (1-based) |
| `-uuid-tz` | `America/Los_Angeles` | Часовой пояс для генерации UUIDv7 |

### Параметры производительности

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `-sw` | кол-во CPU | Количество stage-воркеров (экспорт данных) |
| `-lw` | кол-во CPU | Количество load-воркеров (импорт данных) |
| `-chunk` | `100000` | Количество строк на один файл/транзакцию |

### Параметры оптимизации InnoDB

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `-innodb-buffer-pool-gb` | `0` | Размер buffer pool в GB (0 = не менять) |
| `-innodb-io-capacity` | `0` | InnoDB IO capacity (0 = не менять) |
| `-innodb-io-capacity-max` | `0` | InnoDB IO capacity max (0 = не менять) |

### Режимы загрузки

| Параметр | По умолчанию | Описание |
|----------|--------------|----------|
| `-fast-load` | `true` | Включить fast-load оптимизации (отключение проверок, binlog, redo log) |
| `-local-infile` | `false` | Использовать LOAD DATA LOCAL INFILE (файлы на клиенте) |

## Архитектура

```
┌─────────────┐
│ Source DB   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────┐
│   Stage Workers (Parallel)      │
│   - Читают данные по chunk'ам   │
│   - Генерируют UUIDv7           │
│   - Записывают в CSV файлы      │
└──────┬──────────────────────────┘
       │
       │ (CSV files)
       │
       ▼
┌─────────────────────────────────┐
│   Load Workers (Parallel)       │
│   - LOAD DATA [LOCAL] INFILE    │
│   - Удаляют файлы после загрузки│
└──────┬──────────────────────────┘
       │
       ▼
┌─────────────┐
│ Dest DB     │
└─────────────┘
```

### Процесс миграции

1. **Определение диапазона**: Находит MIN/MAX ID в источнике и целевой БД
2. **Разбивка на шарды**: Делит диапазон на chunk'и указанного размера
3. **Stage-фаза** (параллельно):
   - Читает данные из источника по chunk'ам
   - Генерирует UUIDv7 на основе timestamp
   - Сохраняет в CSV файлы
4. **Load-фаза** (параллельно):
   - Загружает CSV через LOAD DATA INFILE
   - Удаляет временные файлы
5. **Статистика**: Выводит время выполнения и скорость

## Fast-Load оптимизации

При включенном `-fast-load=true` выполняются следующие оптимизации:

### Отключаются проверки
```sql
SET GLOBAL unique_checks = 0;
SET GLOBAL foreign_key_checks = 0;
```

### Отключается синхронизация
```sql
SET GLOBAL innodb_flush_log_at_trx_commit = 2;
SET GLOBAL sync_binlog = 0;
SET SESSION sql_log_bin = 0;
```

### Отключается REDO LOG
```sql
ALTER INSTANCE DISABLE INNODB REDO_LOG;
```

### Применяются пользовательские настройки
- `innodb_buffer_pool_size`
- `innodb_io_capacity`
- `innodb_io_capacity_max`

После завершения миграции все настройки восстанавливаются в исходное состояние.

## LOAD DATA LOCAL INFILE

Для использования режима `-local-infile` необходимо:

### 1. Включить на сервере MySQL

```sql
SET GLOBAL local_infile = 1;
```

Или в конфигурации `/etc/my.cnf`:
```ini
[mysqld]
local_infile = 1

[mysql]
local_infile = 1
```

### 2. Перезапустить MySQL (при изменении конфига)

```bash
sudo systemctl restart mysql
```

### 3. Запустить мигратор с флагом

```bash
./logs-migrator -src-dsn "..." -dst-dsn "..." -local-infile
```

## Примеры использования

### Миграция каждой 100-й записи (для тестирования)

```bash
./logs-migrator \
  -src-dsn "root:root@tcp(old-server:3306)/logs" \
  -dst-dsn "root:root@tcp(new-server:3306)/logs" \
  -src-filter "id % 100 = 0" \
  -chunk=10000
```

### Удаленная миграция с оптимизацией

```bash
./logs-migrator \
  -src-dsn "user:pass@tcp(10.0.1.100:3306)/prod_db" \
  -dst-dsn "user:pass@tcp(10.0.2.100:3306)/new_db" \
  -local-infile \
  -sw=8 \
  -lw=4 \
  -chunk=2000000 \
  -innodb-buffer-pool-gb=8 \
  -innodb-io-capacity=3000 \
  -innodb-io-capacity-max=6000 \
  -fast-load=true
```

### Миграция без fast-load (с binlog)

```bash
./logs-migrator \
  -src-dsn "root:root@tcp(source:3306)/db" \
  -dst-dsn "root:root@tcp(dest:3306)/db" \
  -fast-load=false \
  -chunk=100000
```
