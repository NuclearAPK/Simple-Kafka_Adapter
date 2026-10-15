# Тестовый стенд Kafka для очередей (share groups)

Один узел Apache Kafka **4.2.2** в режиме KRaft. Используется для проверки чтения очередью
(KIP-932, спецификация `specs/share-consumer.md`). Версия 4.2.x выбрана намеренно: это минимальная
версия брокера, на которой компонента поддерживает очереди.

Особенности конфигурации:

- брокер доступен на `127.0.0.1:9092` без шифрования и аутентификации. С хоста подключаться
  по `127.0.0.1`, не по `localhost`: на Windows `localhost` сначала резолвится в `::1`, Docker
  на нём не отвечает, и подключение висит около 21 секунды;
- `share.version=1` в 4.2 включается автоматически, отдельная настройка не нужна;
- блокировка записи `group.share.record.lock.duration.ms` уменьшена с 30 до **15 секунд**,
  чтобы проверка истечения блокировки шла быстрее;
- лимит доставок `group.share.delivery.count.limit` — по умолчанию, 5.

## Запуск и остановка

```powershell
cd docker/kafka-share
docker compose up -d
docker compose down          # остановить; данные удаляются вместе с контейнером
```

Тома нет: при пересоздании контейнера (в том числе `docker compose up -d` после правки
`docker-compose.yml`) топики и группы пропадают, подготовку ниже нужно повторить.

## Пробник share consumer

`tests/share-probe` — программа на C API librdkafka, проверяет поведение, на которое
опирается компонента. Создаёт свои топик и группу при каждом запуске.

```powershell
cmake -S tests/share-probe -B out/build/share-probe -G Ninja -DCMAKE_BUILD_TYPE=Release `
  -DCMAKE_TOOLCHAIN_FILE=C:/Sources/vcpkg/scripts/buildsystems/vcpkg.cmake `
  -DVCPKG_TARGET_TRIPLET=x64-windows-static
cmake --build out/build/share-probe
out/build/share-probe/share_probe.exe 127.0.0.1:9092
```

## Проверка компоненты (native harness)

`tests/native-harness` загружает `SimpleKafka1C.dll` так же, как платформа 1С, и прогоняет
критерии приёмки из `specs/share-consumer.md` против стенда. Топики и группы создаются заново
при каждом запуске, ручная подготовка не нужна.

```powershell
cmake -S tests/native-harness -B out/build/native-harness -G Ninja -DCMAKE_BUILD_TYPE=Release `
  -DCMAKE_TOOLCHAIN_FILE=C:/Sources/vcpkg/scripts/buildsystems/vcpkg.cmake `
  -DVCPKG_TARGET_TRIPLET=x64-windows-static
cmake --build out/build/native-harness
out/build/native-harness/native_harness.exe out/build/x64-Release/SimpleKafka1C.dll 127.0.0.1:9092
# только часть сценариев
out/build/native-harness/native_harness.exe out/build/x64-Release/SimpleKafka1C.dll --only AC-1,AC-18
```

AC-15 (брокер без share groups) пропускается, пока не задан `--no-share-brokers`. Чтобы
проверить его на этом же стенде, share groups выключаются на время прогона:

```powershell
docker exec kafka-share /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9092 `
  downgrade --feature share.version=0
out/build/native-harness/native_harness.exe out/build/x64-Release/SimpleKafka1C.dll `
  --only AC-15 --no-share-brokers 127.0.0.1:9092
docker exec kafka-share /opt/kafka/bin/kafka-features.sh --bootstrap-server localhost:9092 `
  upgrade --feature share.version=1
```

Код возврата 0 — все проверки прошли. Логи компоненты — в `%TEMP%
ative-harness-<время>`.

## Подготовка топика и группы

Команды ниже — для PowerShell. В Git Bash перед ними выполнить `export MSYS_NO_PATHCONV=1`,
иначе пути `/opt/kafka/...` будут переписаны в пути Windows.

```powershell
docker exec kafka-share /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 `
  --create --topic q-test --partitions 1

# Новая share group по умолчанию читает только сообщения, записанные после её создания
# (share.auto.offset.reset=latest). Для тестов группе ставится earliest.
docker exec kafka-share /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 `
  --entity-type groups --entity-name sg-test --alter --add-config share.auto.offset.reset=earliest
```

## Проверка стенда

```powershell
# Записать сообщения
"m1","m2","m3" | docker exec -i kafka-share /opt/kafka/bin/kafka-console-producer.sh `
  --bootstrap-server localhost:9092 --topic q-test

# Прочитать очередью (консольный share consumer, 20 секунд)
docker exec kafka-share timeout 20 /opt/kafka/bin/kafka-console-share-consumer.sh `
  --bootstrap-server localhost:9092 --topic q-test --group sg-test `
  --formatter-property print.offset=true

# Список share groups и их состояние
docker exec kafka-share /opt/kafka/bin/kafka-share-groups.sh --bootstrap-server localhost:9092 --list
docker exec kafka-share /opt/kafka/bin/kafka-share-groups.sh --bootstrap-server localhost:9092 `
  --describe --group sg-test

# Вернуть группу к началу топика (группа не должна иметь активных участников)
docker exec kafka-share /opt/kafka/bin/kafka-share-groups.sh --bootstrap-server localhost:9092 `
  --reset-offsets --group sg-test --topic q-test --to-earliest --execute
```
