# Сборка компоненты под RHEL 9

Контейнерная сборка `SimpleKafka1C.so` для RHEL 9 со статической линковкой всех
сторонних зависимостей.

## Что получается

- `out/rhel9/SimpleKafka1C.so` — библиотека Native API для 1С:Предприятие;
- `out/rhel9/build-info.txt` — версии тулчейна, коммит vcpkg, sha256, вывод
  `ldd`, список требуемых версий символов glibc и список экспортируемых символов.

Статически внутрь `.so` линкуются: librdkafka, OpenSSL, curl, protobuf, abseil,
utf8-range, boost (json, container, property-tree), avro-cpp, fmt, zlib, zstd,
lz4, snappy, cyrus-sasl, krb5, libuuid, а также `libstdc++`.

Внешние зависимости результата:

```
linux-vdso.so.1
libresolv.so.2
libm.so.6
libgcc_s.so.1
libc.so.6
/lib64/ld-linux-x86-64.so.2
```

Максимальная требуемая версия символов — `GLIBC_2.34`, то есть библиотека
загружается на любом RHEL 9 (glibc 2.34), включая 9.0.

`libgcc` намеренно линкуется динамически: с `-static-libgcc` внутрь попадает
раскрутчик стека, который ссылается на `_dl_find_object@GLIBC_2.35`, а этого
символа нет в ранних минорных версиях RHEL 9. Библиотека `libgcc_s.so.1` есть
в базовой поставке любой системы RHEL 9.

## Запуск

Требуется Docker с BuildKit (по умолчанию в Docker 23+).

Windows:

```powershell
pwsh docker/rhel9/build.ps1
```

Linux/macOS:

```sh
sh docker/rhel9/build.sh
```

Напрямую:

```sh
docker build -f docker/rhel9/Dockerfile --target export --output out/rhel9 .
```

Первая сборка занимает значительное время: vcpkg собирает все зависимости из
исходников. Повторные сборки используют кеш слоёв Docker и пересобирают только
саму компоненту.

## Параметры

| Параметр | По умолчанию | Назначение |
|---|---|---|
| `BASE_IMAGE` | `rockylinux/rockylinux:9` | Базовый образ сборки |
| `VCPKG_REF` | `master` | Коммит или тег vcpkg — фиксирует версии зависимостей |
| `BUILD_TYPE` | `Release` | Тип сборки CMake |
| `TRIPLET` | `x64-linux` | Триплет vcpkg (на Linux это статическая линковка) |

Пример фиксации версий зависимостей:

```sh
VCPKG_REF=2025.06.13 sh docker/rhel9/build.sh
```

## О базовом образе

Rocky Linux 9 бинарно совместим с RHEL 9: та же версия glibc (2.34), тот же
gcc 11.x и те же версии символов. Собранная библиотека работает на RHEL 9,
CentOS Stream 9, AlmaLinux 9, Rocky Linux 9 и более новых системах с glibc ≥ 2.34.

Для сборки строго на образе Red Hat:

```sh
BASE_IMAGE=registry.access.redhat.com/ubi9/ubi:latest sh docker/rhel9/build.sh
```

В UBI-образе часть пакетов доступна только из репозитория
`ubi-9-codeready-builder-rpms`, поэтому набор пакетов в `Dockerfile` может
потребовать корректировки.

## Правки, применяемые только на время сборки

Каталог `patches/` содержит изменения, которые накладываются на копию исходников
внутри контейнера; репозиторий остаётся нетронутым.

- `0001-drop-protobuf-protoc-pre-declaration.patch` — убирает из `CMakeLists.txt`
  обход старого бага экспорта vcpkg вокруг `protobuf::protoc`. С актуальным
  vcpkg он ломает конфигурацию дважды: предварительное объявление цели приводит
  к `Some (but not all) targets in this export set were already defined`, а
  переопределение `add_executable()` уходит в бесконечную рекурсию с
  одноимённым переопределением из toolchain-файла vcpkg.

Кроме того, в команду `cmake` добавлены два флага, которых нет в
`CMakeLists.txt`:

- `-include fmt/format.h` — заголовки avro-cpp 1.12.1 вызывают `fmt::format()`,
  подключая только `<fmt/base.h>`; начиная с fmt 11 объявление находится в
  `<fmt/format.h>`;
- `-luuid` — `include/com.h` (Native API) использует `uuid_compare()` и
  `uuid_parse()`, линкуется статическая libuuid из vcpkg.

Если эти правки перенести в `CMakeLists.txt`, каталог `patches/` и
дополнительные флаги станут не нужны.

## Проверка результата на целевом сервере

```sh
ldd SimpleKafka1C.so                       # список внешних зависимостей
objdump -T SimpleKafka1C.so | grep GLIBC_  # требуемые версии glibc
nm -D --defined-only SimpleKafka1C.so      # экспортируемые символы Native API
```

Экспорт ограничен файлом `src/version.script`: наружу выходят только
`GetClassObject`, `DestroyObject`, `GetClassNames` и `SetPlatformCapabilities`.
