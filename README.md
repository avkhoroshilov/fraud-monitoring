# Мониторинг потокового антифрод сервиса

## Запуск

Нужны Docker Engine с Compose v2 либо Docker Desktop, примерно 8 ГБ доступной памяти и свободные порты из таблицы ниже. Все сервисы используют общую сеть Compose. Node Exporter рассчитан на Linux, в Docker Desktop он показывает Linux VM Docker, а не macOS или Windows.

Скачайте код проекта или распакуйте архив решения. Из каталога с docker-compose.yml:

```bash
mkdir -p data
cp .env.example .env
```

Положите данные в data/train.csv и data/test.csv. Дополнительно скачивать или генерировать данные не нужно. train.csv используется для обучения, test.csv для непрерывного потока Kafka. CSV не включены в Git.

Названия мерчантов с запятыми в кавычках читаются стандартным CSV парсером. Отдельная колонка transaction_id не требуется: идентификаторы создаются автоматически. Сортировать CSV по времени не нужно. Для переобучения нужны минимум три последовательных месяца с обоими классами в обучающем окне и следующий месяц для проверки кандидата. Для исходного датасета это январь апрель 2019 года и далее.

```bash
docker compose up -d --build --remove-orphans --wait --wait-timeout 600
```

Первый запуск скачивает образы, читает весь train.csv порциями и создаёт начальную модель. volume-init, data-init, kafka-init, airflow-init завершаются с кодом 0. Остальные сервисы продолжают работать. Симулятор отправляет новые события бесконечно, по умолчанию каждые 0.1 секунды, у каждого новый ID и текущее время. Метрики обновляются с потока.

| Сервис | Адрес | Доступ |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | без пароля |
| Метрики скоринга | http://localhost:8000/metrics | без пароля |
| Метрики записи | http://localhost:8001/metrics | без пароля |
| Airflow | http://localhost:8081 | admin / admin |

Интерфейсы привязаны к localhost. Пароли Grafana и Airflow настраиваются в .env до первого запуска. При сохранённом томе изменение переменной не меняет уже созданного пользователя Grafana. PostgreSQL и Node Exporter доступны только внутри Compose. Источники данных, дашборды и правила загружаются через provisioning, ручная настройка не нужна.

## Дашборды

Откройте папку Fraud Monitoring в Grafana. По умолчанию показаны последние 15 минут с обновлением раз в 2 секунды. Для rate и p95 дождитесь хотя бы 30 секунд потока.

| Дашборд | Прямая ссылка | Содержимое |
|---|---|---|
| 01 Scoring and model updates | http://localhost:3000/d/fraud-scoring | TPS, p95 обработки, доля фрода, heatmap скоров, возраст последнего результата, смены модели и ошибки |
| 02 PostgreSQL writes | http://localhost:3000/d/fraud-writer | TPS и p95 записи, число фродовых результатов, heatmap скоров, дубликаты, SQL-срез числа скоров и доли фрода по версии модели |
| 03 Host system | http://localhost:3000/d/fraud-system | CPU, память, сеть по интерфейсам exporter, занятое место, чтение и запись диска, доступность targets |
| 04 Business segments | http://localhost:3000/d/fraud-business | Плотность скоров и TPS с фильтрами US State и Merchant, фрод по категориям последних 1000 транзакций, суммы подозрительных транзакций по мерчантам |

На Business segments фильтры US State и Merchant поддерживают несколько значений и All, список мерчантов зависит от штатов. Они применяются к плотности, TPS и суммам подозрительных транзакций. Значения подставляются через sqlstring с экранированием апострофов. SQL выполняется пользователем grafana_reader с правом SELECT. Мерчанты не попадают в labels Prometheus.

Плотность: 20 бинов шириной 0.05, высота count / total / 0.05, площадь равна 1 для непустой выборки. TPS использует фактическое время скоринга processed_at, окна по 10 секунд. Последнее неполное окно может показывать меньшую скорость.

Barplot по cat_id сначала выбирает последние 1000 уникальных записанных транзакций по ingestion_id, затем считает AVG(fraud_flag) по категориям. Он не зависит от времени и фильтров сегмента; при прогреве используются все доступные строки. Собственный график Top 10 merchants by flagged transaction amount показывает SUM(amount * fraud_flag) для приоритизации ручной проверки по объёму подозрительных платежей. Единицы суммы берутся из CSV, это объём помеченных платежей, а не подтверждённый ущерб. Доли фрода в проекте означают предсказания модели, не фактическую долю мошенничества.

## Метрики

Скоринг обновляет метрики после подтверждения результата Kafka, writer после commit PostgreSQL. Счётчики относятся к жизни процесса. Долговременные итоги уникальных транзакций доступны в PostgreSQL.

| Метрика | Назначение | Панель |
|---|---|---|
| fraud_scoring_transactions_total | Успешно отскоренные и подтверждённые Kafka транзакции | Scored transactions per second |
| fraud_scoring_processing_seconds | Гистограмма времени одного события, включая подтверждение Kafka | Processing latency p95 |
| fraud_scoring_score | Гистограмма скоров 0…1 с шагом 0.05 | Score distribution over time |
| fraud_scoring_fraud_total | Счётчик фродовых результатов | Fraud share over 30 seconds |
| fraud_scoring_fraud_ratio | Доля фрода последних до 1000 результатов в памяти | Prometheus; временная доля на дашборде считается через rate |
| fraud_scoring_last_success_timestamp_seconds | Время последнего результата, до первого события 0 | Seconds since last scored event |
| fraud_scoring_errors_total{reason} | Ошибки входа и доставки | Scoring errors by reason |
| fraud_model_updates_total{version} | Успешные горячие загрузки новой версии без холодного старта | Hot loads by model version |
| fraud_model_reload_timestamp_seconds{version} | Фактическое время горячей загрузки | Аннотация Model hot reload |
| fraud_model_active{version} | 1 у загруженной версии, 0 у прежних | Prometheus |
| fraud_model_reload_errors_total | Ошибки проверки SHA256 и загрузки модели | Failed model reload attempts |
| fraud_writer_rows_total | Новые committed строки | New committed scores per second |
| fraud_writer_commit_seconds | Гистограмма времени SQL и commit Kafka-сообщения | Database commit latency p95 |
| fraud_writer_fraud_total | Новые committed строки с fraud_flag=1 | Fraud rows committed since writer start |
| fraud_writer_score | Гистограмма записанных скоров | Committed score distribution over time |
| fraud_writer_duplicates_total | Повторы, пропущенные по transaction_id | Duplicate deliveries skipped |
| fraud_writer_errors_total{reason} | Невалидный вход и ошибки БД | Writer errors per second |
| node_cpu_seconds_total, node_memory_* | CPU и память | Host system |
| node_network_*, node_filesystem_*, node_disk_* | Сеть, место и I/O диска | Host system |

У гистограмм также есть _bucket, _count, _sum. p95 рассчитывается через histogram_quantile, heatmap декумулирует бакеты в Grafana. Метрики не содержат transaction_id, штаты или мерчантов в labels.

Новая метрика: fraud_scoring_last_success_timestamp_seconds. Выражение time() - fraud_scoring_last_success_timestamp_seconds на панели Seconds since last scored event обнаруживает остановку полезной работы при отвечающем HTTP endpoint. Timestamp=0 до первого события означает отсутствие результатов. Дополнительно реализованы метрики дубликатов записи и ошибок горячей загрузки.

## Проверка потока

1. Откройте http://localhost:9090/targets: scorer, scoring-writer, node-exporter должны быть UP.
2. Дважды откройте оба адреса /metrics с интервалом несколько секунд: fraud_scoring_transactions_total и fraud_writer_rows_total растут. В Grafana на первых двух дашбордах появляются TPS и распределения.
3. На Business segments выберите штат и мерчанта: плотность и TPS пересчитаются. Переключите All для всей выборки.
4. В Stored scores and fraud share by model после переобучения появятся строки новой версии.

## Переобучение и отметка версии

В Airflow выберите DAG retrain_fraud и нажмите Trigger DAG. Он готовит временное окно, обучает sklearn Pipeline, сравнивает кандидата с действующей моделью на одном проверочном месяце и публикует принятую модель. DAG supply_labeled_batch поставляет новые размеченные месяцы. Детали временного разбиения и публикации в [docs/retraining.md](docs/retraining.md).

Начальная версия initial использует DummyClassifier с частотой класса из train. Первый принятый кандидат заменяет её LogisticRegression. Зелёный DAG с rejected не меняет версию, поэтому отметки в таком случае нет. Если следующего месяца нет, обучение пропускается. Gate ради отметки не обходится.

Публикация атомарно сохраняет модель, затем одной SQLite-транзакцией меняет active и следующий месяц. Скорер проверяет реестр перед каждым сообщением, сверяет SHA256 и загружает Pipeline без перезапуска. Модель и версия заменяются вместе. При повреждённом обновлении остаётся прежняя модель и растёт fraud_model_reload_errors_total.

На 01 Scoring and model updates аннотация Model hot reload показывает новую версию и интервал около 30 секунд. Запрос: fraud_model_reload_timestamp_seconds > (time() - 30). Timestamp ставится при загрузке: отметка не зависит от того, видел ли Prometheus ноль счётчика с новой меткой. Интервал доступен и в истории дашборда. Холодный старт не считается горячей сменой.

Дождитесь завершения DAG, затем откройте дашборд скоринга: должна появиться отметка новой версии. На дашборде записи в PostgreSQL новые результаты будут относиться к этой версии.

## Алерты Grafana

monitoring/grafana/provisioning/alerting/rules.json загружается при старте. Правила проверяются каждые 10 секунд и видны в http://localhost:3000/alerting/list. Доставка уведомлений не требуется и не настраивается.

| Правило | Порог | Удержание |
|---|---|---|
| Fraud share above 40 percent | Доля фрода через rate за 30 секунд > 0.40 | 30 секунд |
| Scoring stream stopped | TPS скоринга через rate за 30 секунд < 0.01 | 30 секунд |

Ошибка выполнения правила переводит его в Alerting. Для отсутствующего скорера запрос TPS возвращает 0 через or vector(0). До появления первого потока возможен временный alert остановки.

Проверка остановки:

```bash
docker compose stop simulator
```

После обработки Kafka backlog и примерно 60–90 секунд откройте Alerting: правило Scoring stream stopped должно стать Firing. Верните поток командой docker compose start simulator, правило вернётся в Normal.

Для высокой доли фрода сначала опубликуйте обученную модель. Остановите обычный симулятор и отправьте строки, которым действующая модель действительно назначает флаг фрода:

```bash
docker compose stop simulator
docker compose exec scorer python scripts/send_fraud_demo.py --seconds 100
docker compose start simulator
```

Во время отправки откройте Alerting: Fraud share above 40 percent переходит в Firing. Скрипт использует строку из вашего train.csv, сохранённую в витрине, и шлёт новые события через Kafka, метрики и флаг вычисляет обычный скорер. Если подходящих строк нет, скрипт сообщает об этом и завершается ошибкой. ID имеют префикс alert-demo- эти данные видны в аналитике.

## Контракты и хранение

Вход transactions: {"transaction_id":"tx-1","data":{...}}. Выход scoring: массив с прежними обязательными полями и необязательными метаданными:

```json
[{"transaction_id":"tx-1","score":0.72,"fraud_flag":1,"model_version":"2019-04-...","metadata":{"processed_at":"2026-11-10T12:00:00+00:00","us_state":"CA","merch":"shop-1","cat_id":"grocery","amount":100.0}}]
```

Inference работает на CPU, порог score >= 0.5. Writer понимает старые сообщения без metadata: для аналитических полей ставит unknown и время приёма.

Скорер подтверждает offset после доставки результата или ошибки в scoring_errors. Writer валидирует весь массив, записывает его одной PostgreSQL-транзакцией, затем подтверждает offset. Невалидный вход отправляется в writer_errors. При сбое БД offset не подтверждается, процесс перезапускается. Уникальный transaction_id и ON CONFLICT DO NOTHING защищают от дубликатов; при повторе с другой версией остаётся первый записанный результат.

Счётчик скоринга отражает обработки, поэтому при повторной доставке может превышать число уникальных строк. Сбой между commit БД и обновлением метрик также может нарушить их абсолютное равенство: durable итоги проверяются через SQL.

Витрина и реестр моделей остаются в SQLite WAL в томе shared. Отдельная база scores-db хранит результаты; postgres обслуживает Airflow. Стенд рассчитан на один Docker-хост. Модель и препроцессинг сохранены из предыдущего собственного задания.

## Структура

| Путь | Назначение |
|---|---|
| fraud_pipeline/stream.py, scoring.py | Поток, CPU inference, горячая загрузка |
| fraud_pipeline/writer.py, sql/ | Идемпотентная запись и SQL-схема |
| fraud_pipeline/metrics.py | Метрики обоих сервисов |
| dags/, fraud_pipeline/tasks/ | DAG обучения и поставки |
| monitoring/prometheus.yml | Три scrape job с интервалом 1 секунда |
| monitoring/grafana/ | Четыре дашборда, два datasource и алерты |
| scripts/send_fraud_demo.py | Поток из реальной строки витрины для проверки алерта высокой доли фрода |

## Диагностика и остановка

```bash
docker compose ps -a
docker compose logs --tail=100 data-init scorer scoring-writer prometheus grafana
docker compose down
```

down сохраняет данные. Полная очистка docker compose down -v удаляет все тома проекта: витрину, модели, scores, историю Airflow, Prometheus и Grafana. Если Merchant везде unknown, используйте CSV с merch и новую витрину: старый том мог не сохранять это поле. Имя проекта fraud-monitoring отделяет стенд от fraud-airflow-csv, остановите предыдущий стенд, если он занимает те же порты.