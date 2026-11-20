# Спецификация: единая обработка неустранимых ошибок подключения (SSL, аутентификация)

Статус: реализовано 2026-11-20 (1.10.3), не закоммичено. Спецификация и план утверждены 2026-11-20.
Проверка — раздел 7.

## 1. Проблема

Пользователи жалуются: `ПолучитьСписокТопиков` для брокера `kafka.internal:9093` по SSL
не возвращает результат, а kafkacat отвечает мгновенно.

Воспроизведено 2026-11-20 на `out/build/x64-Release/SimpleKafka1C.dll` (librdkafka 2.16.0):

| Настройки | Время | Результат |
|---|---|---|
| по умолчанию | 10 039 мс (= таймаут) | пусто, `Local: Broker transport failure` |
| `ssl.endpoint.identification.algorithm=none` | 191 мс | 3 топика |

Причина на стороне стенда: сертификат брокера выдан на `CN = kafka`, без SAN, а librdkafka ≥ 2.0
по умолчанию проверяет имя хоста (`ssl.endpoint.identification.algorithm=https`). Цепочка
доверия при этом валидна (`openssl verify -CAfile ca-cert.txt` — OK). Дополнительно брокер
анонсирует `localhost:9093` (`advertised.listeners`) — это настройка брокера, не компоненты.

Дефект компоненты не локален для одного метода. Ошибка рукопожатия SSL или аутентификации
приходит из фонового потока librdkafka через ~50 мс, после чего librdkafka бесконечно
повторяет попытки. Любой блокирующий вызов компоненты в это время:

1. **ждёт до конца своего таймаута** (метаданные, Admin API, flush, транзакции, чтение);
2. **возвращает бесполезный текст** (`Local: Broker transport failure`, `Local: Timed out`)
   или вообще не сообщает об ошибке (чтение возвращает «нет сообщений»);
3. **не различает** временную недоступность брокера и неустранимую ошибку конфигурации.

Текущее состояние по группам:

| Группа | Подписка на ошибки | Что видит 1С при сбое SSL |
|---|---|---|
| Служебные клиенты (метаданные, `AdminClientScope`, watermarks, группы, ACL) | нет | ожидание до таймаута, `Broker transport failure` |
| Producer, транзакции | `event_cb` → `asyncErrorState` | ожидание до `message.timeout.ms`/таймаута, текст только через `ПолучитьСообщениеОбОшибке()` без связи с вызовом |
| Consumer (`Прочитать`, пакетное чтение, `ПолучитьСообщение`) | `event_cb` → `asyncErrorState` | «нет сообщений», ожидание каждого вызова |
| Share consumer | `error_cb` → свой лог | «нет сообщений» |
| Push consumer (ExternalEvent) | `event_cb` | события об ошибке нет, фоновый поток читает вхолостую |
| Schema Registry (curl) | — | ошибка curl без подсказки про имя хоста |

## 2. Требования

### 2.1. Классификация ошибок

R1. Единый классификатор `ConnectFailure` для ошибок librdkafka:
    - **неустранимые**: `ERR__SSL`, `ERR__AUTHENTICATION`, `ERR_SASL_AUTHENTICATION_FAILED`,
      `ERR_TOPIC_AUTHORIZATION_FAILED`/`ERR_CLUSTER_AUTHORIZATION_FAILED`/
      `ERR_GROUP_AUTHORIZATION_FAILED`/`ERR_TRANSACTIONAL_ID_AUTHORIZATION_FAILED`,
      а также любая ошибка с флагом fatal;
    - **временные**: `ERR__TRANSPORT`, `ERR__ALL_BROKERS_DOWN`, `ERR__RESOLVE`, прочие.
    Перед реализацией коды сверить с `rdkafka.h` и местом возникновения в `rdkafka_ssl.c` /
    `rdkafka_sasl*.c` librdkafka 2.16.0.

R2. Неустранимая ошибка записывается в общее для клиента состояние: код, текст, время.
    Для долгоживущих клиентов — в существующий `asyncErrorState` (новое поле `failure`),
    для служебных — в состояние, которое создаёт `AdminClientScope`/`createMetadataClient`.

### 2.2. Текст ошибки

R3. Формат `msg_err`: `<Операция>: <краткая причина> | <текст librdkafka>[ | <подсказка>]`,
    например `GetTopics: SSL handshake failed | ssl://kafka.internal:9093/bootstrap: SSL
    handshake failed: ... certificate verify failed ... | <подсказка>`.

R4. Подсказки (одна общая функция, используется и для librdkafka, и для curl):
    - `certificate verify failed` при `ssl.endpoint.identification.algorithm` ≠ `none` →
      «Возможно, имя хоста <host> не совпадает с CN/SAN сертификата брокера. Перевыпустите
      сертификат с SAN или задайте ssl.endpoint.identification.algorithm=none (ослабляет защиту)»;
    - `certificate verify failed` при `none` → «Проверьте ssl.ca.location: CA не подтверждает
      сертификат брокера»;
    - ошибка чтения ключа/сертификата клиента → «Проверьте ssl.key.location /
      ssl.certificate.location / ssl.key.password»;
    - SASL → «Проверьте sasl.mechanism, sasl.username, sasl.password».

R5. По таймауту при только временных ошибках к тексту таймаута добавляется последняя
    асинхронная ошибка: `Local: Broker transport failure | <текст>`.

### 2.3. Быстрый отказ блокирующих вызовов

R6. Все блокирующие вызовы ждут отрезками ≤ 500 мс в пределах исходного таймаута и после
    каждого отрезка проверяют состояние из R2. При неустранимой ошибке вызов прекращается
    и возвращает значение ошибки своего типа (`Ложь`, `-1`, пустая строка — по
    `specs/error-result-values.md`). Таймауты и сигнатуры методов не меняются.

R7. Область R6:
    - служебные клиенты: все `metadata()`/`rd_kafka_metadata()`, `query_watermark_offsets`,
      `offsetsForTimes`, `committed`, ожидание результата Admin API в `rd_kafka_queue_poll`;
    - producer: синхронная отправка и ожидание доставки, `flush`, пакетная отправка;
    - транзакции: `init_transactions`, `commit_transaction`, `abort_transaction`;
    - consumer: чтение по одному и пакетом, `ПолучитьСообщение`, установка позиций;
    - share consumer: чтение и подтверждение;
    - Avro/Protobuf методы, которые обращаются к брокеру.

R8. Неустранимая ошибка «липкая» для долгоживущего клиента: пока он не пересоздан
    (`Инициализировать...`/`Остановить...`), каждый блокирующий вызов сразу возвращает
    ошибку с тем же текстом, без ожидания. Временные ошибки не липкие.

### 2.4. Фоновое чтение (push consumer)

R9. При неустранимой ошибке push consumer отправляет в 1С `ExternalEvent` с типом ошибки
    (формат — как у существующих событий ошибок в `docs/`) и останавливает фоновый цикл.
    Повторных событий об одной и той же ошибке нет.

### 2.5. Schema Registry

R10. При ошибке TLS curl (`CURLE_PEER_FAILED_VERIFICATION`, `CURLE_SSL_CACERT`,
     `CURLE_SSL_CERTPROBLEM`) в `msg_err` попадает текст curl и подсказка из R4.
     Быстрый отказ здесь уже есть — curl не повторяет попытки.

### 2.6. Совместимость

R11. При успешном подключении, без SSL/SASL и при временной недоступности брокера
     поведение и время ответа не меняются (кроме более подробного текста по R5).
R12. Новых зависимостей нет. Новых публичных методов нет.

## 3. Вне области

- Исправление сертификата и `advertised.listeners` брокера `kafka.internal` (передать
  администратору стенда).
- Автоматическое переподключение после исправления конфигурации — пользователь
  пересоздаёт клиента сам (R8).

## 4. Критерии приёмки

Стенд: брокер `kafka.internal:9093` (сертификат без SAN), сертификаты `C:\Temp\denvic`;
для SASL и неверного CA — Docker-стенд проекта. Проверка — native harness.

AC-1. Given SSL, сертификат без SAN, настройки по умолчанию,
      When `ПолучитьСписокТопиков("kafka.internal:9093", 10000)`,
      Then пустая строка не позже 1000 мс,
      And ошибка содержит `SSL handshake failed` и подсказку про CN/SAN.

AC-2. Given те же условия, When `ПроверитьБрокер`, `ПолучитьИнформациюОКластере`,
      создание топика, список групп, Then каждый возвращает значение ошибки не позже 1000 мс
      с тем же классом текста.

AC-3. Given producer инициализирован с теми же настройками,
      When синхронная отправка сообщения с таймаутом 10 с,
      Then `Ложь` не позже 1000 мс, ошибка с `SSL handshake failed`;
      And повторная отправка возвращает ту же ошибку сразу (R8).

AC-4. Given транзакционный producer, When `НачатьТранзакцию`/инициализация транзакций,
      Then ошибка не позже 1000 мс.

AC-5. Given consumer подписан на топик, When `Прочитать` с таймаутом 10 с,
      Then значение ошибки (не «нет сообщений») не позже 1000 мс, текст с `SSL handshake failed`.

AC-6. Given push consumer запущен, When происходит сбой SSL,
      Then в 1С приходит ровно одно `ExternalEvent` об ошибке и фоновое чтение останавливается.

AC-7. Given share consumer, When чтение, Then ошибка не позже 1000 мс.

AC-8. Given неверный SASL-пароль (Docker-стенд), When `ПолучитьСписокТопиков`,
      Then ошибка не позже 1000 мс с подсказкой про `sasl.*`.

AC-9. Given неверный `ssl.ca.location` и `ssl.endpoint.identification.algorithm=none`,
      Then подсказка про `ssl.ca.location`, без подсказки про CN/SAN.

AC-10. Given Schema Registry по HTTPS с сертификатом без SAN,
       When запрос схемы, Then ошибка curl с подсказкой про CN/SAN.

AC-11. Given `ssl.endpoint.identification.algorithm=none` (рабочая конфигурация),
       When все методы из AC-1…AC-7, Then успех, время как в 1.10.2 (≈ 200 мс для списка топиков).

AC-12. Given PLAINTEXT, недоступный порт, When `ПолучитьСписокТопиков(..., 3000)`,
       Then ожидание ≈ 3000 мс (без быстрого отказа), ошибка содержит
       `Broker transport failure` и текст последней асинхронной ошибки.

AC-13. Given доступный брокер без SSL, When полный прогон native harness,
       Then все существующие сценарии проходят.

## 5. Документация

- `docs/`: новый раздел «Ошибки подключения» — классы ошибок, быстрый отказ, липкость,
  тексты подсказок, событие push consumer, рецепт с `ssl.endpoint.identification.algorithm`
  и предупреждение об ослаблении защиты; ссылка из `README.md`.
- Обновить описания затронутых методов, где меняется поведение при ошибке.
- `CHANGELOG.md`: `Fixed` + `Changed`, версия 1.10.3.

## 6. План реализации

### 6.1. Что подтверждено в исходниках librdkafka 2.16.0

- `src/rdkafka_ssl.c:728-770`: при сбое рукопожатия вызывается
  `rd_kafka_broker_fail(rkb, LOG_ERR, RD_KAFKA_RESP_ERR__SSL, "SSL handshake failed: ...")`.
  Обрыв соединения во время рукопожатия даёт `ERR__TRANSPORT`, а не `ERR__SSL`
  («connecting to a PLAINTEXT broker listener?»). Он остаётся временной ошибкой.
- `src/rdkafka_broker.c:684`: ошибка уровня `LOG_ERR` попадает в очередь `rk_rep`
  (`rd_kafka_q_op_err`). Повторы с тем же текстом подавляются, до приложения доходит первая.
- Следствие: `error_cb`/`event_cb` срабатывает **только** внутри `rd_kafka_poll()`,
  `flush()`, `consume()`. Блокирующий `metadata()` очередь `rk_rep` не обслуживает. Поэтому
  ожидание нарезается на отрезки, а между отрезками вызывается `rd_kafka_poll(rk, 0)`.
- Текст librdkafka не отличает ошибку имени хоста от неверного CA: в обоих случаях
  `certificate verify failed` / `error:0A000086`. Подсказка R4 выбирается по настройке
  `ssl.endpoint.identification.algorithm`.

### 6.2. Архитектура

Новый модуль `src/connect_failure.h` / `src/connect_failure.cpp` (добавить в `CMakeLists.txt`):

```cpp
enum class FailureClass { None, Transient, Fatal };
FailureClass classifyKafkaError(int err, bool fatalFlag);           // R1

struct ConnectFailureState {                                         // R2, потокобезопасно
    void record(int err, const std::string& text, bool fatalFlag);   // из колбэков
    bool hasFatal() const;
    std::string fatalText() const;   // первая неустранимая ошибка
    std::string lastText() const;    // последняя любая ошибка (для R5)
    void reset();
};

std::string connectHint(const std::string& text, const SettingsView& s); // R4
std::string formatConnectError(std::string_view op, const ConnectFailureState&,
                               const SettingsView&);                      // R3, R5

// R6: повторяет step(sliceMs) до успеха/таймаута; между отрезками pump() и проверка fatal
template <class Step, class Pump>
StepResult waitSliced(int32_t timeoutMs, ConnectFailureState&, Step, Pump);
```

`SettingsView` — лёгкое чтение `security.protocol`, `ssl.endpoint.identification.algorithm`,
`sasl.mechanism` из вектора `settings`, без копии конфигурации.

### 6.3. Шаги

Каждый шаг заканчивается сборкой и прогоном пробника/harness (раздел 6.4).

1. **Модуль `connect_failure`.** Классификатор, состояние, подсказки, форматирование,
   `waitSliced`. Юнит-проверки классификатора и подсказок — в `tests/error-result`
   (существующий тестовый exe без брокера).
2. **Служебные клиенты.**
   - `createMetadataClient` и `getListOfTopics` (`admin_metadata.cpp`, `admin_topics.cpp`):
     владелец клиента получает `ConnectFailureState` и `EventCb`, который пишет в него;
     `metadata()`/`query_watermark_offsets()` через `waitSliced` с `producer->poll(0)`.
     `createMetadataClient` возвращает пару «клиент + состояние» (структура `MetadataClient`).
   - `AdminClientScope` (`SimpleKafka1C.h`, `admin_topics.cpp`): `rd_kafka_conf_set_error_cb`
     с `opaque` на своё состояние; `rd_kafka_metadata()` и все `rd_kafka_queue_poll(admin.queue(), …)`
     в `admin_topics.cpp`, `admin_groups.cpp`, `admin_acl.cpp` — через `waitSliced`
     с `rd_kafka_poll(rk, 0)`. Новый метод `AdminClientScope::pollResult(timeout)` убирает
     дублирование в ~15 местах.
   - `consumer_methods.cpp:177,228` — тот же `MetadataClient`.
   - Временный consumer в `getPartitionWatermarks`/чтении последнего сообщения
     (`admin_metadata.cpp:439,776`) — `consume()` отрезками с проверкой состояния.
   - Исправить утечку `metadata` на пути ошибки (там, где она есть).
   → AC-1, AC-2, AC-12.
3. **Долгоживущие клиенты: общее состояние.** `clEventCb::event_cb` дополнительно пишет
   в `ConnectFailureState` (поле в `AsyncErrorState`). Сброс состояния — в `initProducer`,
   `initConsumer`, `stopProducer`, `stopConsumer`. Проверка «липкой» ошибки (R8) в начале
   каждого блокирующего метода producer/consumer.
4. **Producer и транзакции** (`producer_methods.cpp`).
   - `flush(...)` в строках 257, 602, 655 — отрезками `flush(≤500)` (flush сам обслуживает
     колбэки).
   - `init_transactions` / `commit_transaction` / `abort_transaction` — отрезками;
     `ERR__TIMED_OUT` у них retriable (сверить с `rdkafkacpp.h`), между вызовами `poll(0)`.
   - Синхронная отправка с ожиданием доставки — ожидание отрезками.
   → AC-3, AC-4.
5. **Consumer** (`consumer_methods.cpp:438,516,705,924`, `admin_groups.cpp`).
   - `consume(waitMessageTimeout)` → цикл `consume(≤500)` до сообщения/таймаута/fatal.
     При fatal метод возвращает значение ошибки, а не «нет сообщений».
   - `committed`, `offsetsForTimes`, `metadata` на `hConsumer` (`admin_groups.cpp`):
     обслужить очередь нельзя без чтения сообщений, поэтому проверяется только липкое
     состояние до вызова и после него + текст R3/R5 при ошибке. Это ограничение
     записать в документацию.
   → AC-5.
6. **Push consumer** (`push_consumer.cpp:198`). После каждого `consume(pollMs)` проверка
   fatal: одно `ExternalEvent` в формате существующих событий ошибок, остановка цикла,
   флаг «уже отправлено». → AC-6.
7. **Share consumer** (`share_consumer_methods.cpp`). `rd_kafka_conf_set_error_cb` уже
   есть (`ShareLogContext::errorCallback`) — добавить запись в `ConnectFailureState`;
   `rd_kafka_share_poll` и `rd_kafka_share_commit_sync` — отрезками с проверкой. → AC-7.
8. **Schema Registry** (`schema_registry_methods.cpp:65`). На кодах curl
   `CURLE_PEER_FAILED_VERIFICATION`, `CURLE_SSL_CACERT_BADFILE`, `CURLE_SSL_CERTPROBLEM`,
   `CURLE_SSL_CONNECT_ERROR` добавить подсказку из `connectHint` (настройку проверки имени
   хоста для curl взять из параметров Schema Registry). Коды сверить с `curl.h` из vcpkg.
   → AC-10.
9. **Документация и версия.** `docs/connection-errors.md` (новый), ссылка в `README.md`,
   правки описаний затронутых методов, `CHANGELOG.md` (1.10.3, `Fixed` + `Changed`),
   `Version[]` в `SimpleKafka1C.h`.

### 6.4. Проверка

- Пробник из этой сессии превращается в сценарии native harness с ключом
  `--ssl-dir C:\Temp\denvic --ssl-brokers kafka.internal:9093`: AC-1…AC-7, AC-9, AC-11.
  Время каждого вызова фиксируется и сравнивается с порогом 1000 мс.
- AC-8 (SASL) и AC-12 (недоступный порт) — Docker-стенд проекта (`docker/`), нужен
  listener SASL_PLAINTEXT; если его нет в compose, добавить в `docker/` отдельным файлом.
- AC-10 — Schema Registry стенда `kafka.internal`, если он доступен по HTTPS;
  иначе локальный HTTPS-заглушкой с сертификатом без SAN (openssl s_server).
- AC-13 — полный прогон существующего native harness на Docker-стенде.
- Сборка Windows (vcvars64) после каждого шага; сборка WSL Ubuntu — в конце.

### 6.5. Риски

- Нарезка `metadata()` отправляет новый запрос на каждый отрезок. На рабочем брокере первый
  отрезок успешен, лишней нагрузки нет; на недоступном — запросы не уходят (нет соединения).
- `init_transactions` после таймаута: если повторный вызов окажется не retriable в 2.16.0,
  шаг 4 оставляет один вызов с полным таймаутом и только проверку липкого состояния.
- Изменение типа, возвращаемого `createMetadataClient`, затрагивает 6+ вызывающих мест —
  делается одним шагом с компиляцией.

## 7. Результаты проверки (2026-11-20)

Сборка Windows x64 Release без предупреждений. Сборка WSL Ubuntu не выполнялась.

| Проверка | Стенд | Результат |
|---|---|---|
| `tests/connect-failure` (юнит: классификатор, подсказки, текст, нарезка, UTF-8) | — | 33/33 |
| `tests/error-result` | — | 12/12 |
| `tests/ssl-fail-fast` AC-1…AC-9, AC-11, AC-12 | `docker/kafka-ssl` (`127.0.0.1:9194/9195`) | 58/58 |
| `tests/ssl-fail-fast` | `kafka.internal:9093`, сертификаты `C:\Temp\denvic` | 44/45, см. ниже |
| AC-10, Schema Registry по HTTPS | `openssl s_server` с сертификатом CN=kafka | ошибка curl + подсказка, без падения |
| AC-13, `tests/native-harness` | `docker/kafka-share` | 392 проверки, 0 провалов |

Время отказа при ошибке SSL/SASL — 500–513 мс (было: полный таймаут, 10 с для списка топиков).
AC-12: ожидание 3000 мс при таймауте 3000 мс.

На `kafka.internal` не проходит только «отправка с `ssl.endpoint.identification.algorithm=none`»:
брокер анонсирует `localhost:9093` (`advertised.listeners`), сообщение уходит на `localhost`
клиента. Так же ведёт себя 1.10.0 — это настройка стенда, не компоненты. По той же причине
`ПолучитьСписокГруппКонсьюмеров` на этом стенде ждёт полный таймаут.

### Отклонения от плана

- Риск 6.5 про `init_transactions` снят: `rdkafka.h` помечает `__TIMED_OUT` у
  `init/commit/abort_transaction` как retriable, транзакции режутся на отрезки.
- Нарезанные вызовы C API вынесены в `connect_failure` (`slicedMetadata`, `slicedWatermarks`,
  `slicedCommitted`, `slicedQueuePoll`, `rdkafkaErrorCallback`); `AdminClientScope` и функция
  отставания группы в `admin_groups.cpp` используют их.
- Producer и consumer делят один `cl_event_cb`, поэтому каждому дана обёртка `TrackingEventCb`
  со своим состоянием; события по-прежнему уходят в `cl_event_cb` (лог и `asyncErrorState`).
- Фоновое чтение сообщает об ошибке существующим событием `Stopped`, отдельный тип не нужен.
- Не реализовано: быстрый отказ временного консьюмера в `ПолучитьГраницыПартиции`
  (чтение last_timestamp) и в расчёте размера топика — они создаются после успешного запроса
  метаданных тем же набором настроек.
- Найдено попутно: schannel на русской Windows пишет подробности ошибки в CP1251, невалидный
  UTF-8 в `msg_err` ронял процесс. Добавлен `connect_failure::ensureUtf8`, через него проходят
  все тексты ошибок подключения. Связано с открытой спецификацией `utf8-tolerant-conversion.md`.

### Новые файлы проверки

- `tests/connect-failure` — юнит-тест модуля.
- `tests/ssl-fail-fast` — приёмочный тест на живом брокере.
- `docker/kafka-ssl` — стенд: SSL-листенер с сертификатом без SAN, SASL PLAIN, генерация
  сертификатов `gen-certs.sh` (каталог `certs/` в `.gitignore`).
