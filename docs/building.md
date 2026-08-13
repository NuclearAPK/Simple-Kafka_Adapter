# Сборка внешней компоненты

Инструкция рассчитана и на человека, и на ИИ-агента: команды даны целиком, каждый шаг заканчивается
проверкой, по которой видно «получилось или нет». Домысливать ничего не нужно — если в команде нет
подстановки в угловых скобках, её можно выполнять как есть.

**Что получается на выходе:** `SimpleKafka1C.dll` (Windows x64) и `libSimpleKafka1C.so` (Linux x64),
упакованные вместе с `MANIFEST.XML` в zip — форму внешней компоненты для 1С.

**Соглашения**
- `<...>` — подставить своё значение. Всё остальное копируется как есть.
- Команды выполняются **из корня репозитория** (в скриптах относительные пути).
- Версия компоненты задаётся в одном месте — `src/SimpleKafka1C.h`, константа `Version`.
  В именах бинарей точки и дефисы меняются на подчёркивания: `1.9.2` → `SimpleKafka1C64_1_9_2.dll`.

---

## 0. Кратко

| Цель | Команда |
|---|---|
| Windows dll | `scripts\build_windows.bat` → `build\Release\SimpleKafka1C.dll` |
| Linux so (с любой ОС, нужен Docker) | `scripts/build_linux.sh` или `scripts\build_linux.bat` → `build/linux/libSimpleKafka1C_Static.so` |
| Убедиться, что собралось именно то | `grep -a -o "<версия>" <путь к бинарю>` |

---

## 1. Что должно быть установлено

| Компонент | Требование | Как проверить |
|---|---|---|
| Git | любой актуальный | `git --version` |
| CMake | >= 3.20 | `cmake --version` |
| Компилятор | Windows: MSVC (Visual Studio 2022 и новее, рабочая нагрузка «Разработка классических приложений на C++»). Linux: gcc/g++ >= 9 | `cl` в среде VS / `g++ --version` |
| vcpkg | клон в `C:\vcpkg` (Windows) или `~/vcpkg` (Linux/macOS) | `<vcpkg>/vcpkg version` |
| Docker | только чтобы собрать Linux-бинарь не на Linux | `docker info` |

> **CMake может не оказаться в `PATH`.** Он ставится вместе с Visual Studio, путь вида
> `C:\Program Files\Microsoft Visual Studio\<год>\<редакция>\Common7\IDE\CommonExtensions\Microsoft\CMake\CMake\bin\cmake.exe`.
> Проще всего запускать сборку из «x64 Native Tools Command Prompt for VS» — там в `PATH` есть и `cl`, и `cmake`.

---

## 2. Windows

### 2.1. Одной командой

```bat
scripts\build_windows.bat
```

Скрипт использует vcpkg из переменной окружения `VCPKG_ROOT`, а если её нет — из `C:\vcpkg`
(клонирует туда, только если каталога не существует). Дальше ставит зависимости под триплет
`x64-windows-static`, **проверяет версию avro-cpp** (см. §5.1) и собирает Release.

> Если vcpkg уже стоит в другом каталоге, укажите его — иначе рядом появится второй экземпляр
> и все зависимости будут собраны заново:
> ```bat
> set VCPKG_ROOT=C:\Sources\vcpkg
> scripts\build_windows.bat
> ```
> `vcpkg integrate install` скрипт не выполняет: эта команда правит user-wide MSBuild props,
> то есть влияет на все проекты на машине, а сборке через `CMAKE_TOOLCHAIN_FILE` не нужна.

### 2.2. То же самое руками

```bat
git clone https://github.com/microsoft/vcpkg C:\vcpkg
call C:\vcpkg\bootstrap-vcpkg.bat
C:\vcpkg\vcpkg install librdkafka avro-cpp protobuf abseil utf8-range boost-property-tree boost-json boost-container snappy fmt curl --triplet x64-windows-static

cmake -B build -A x64 -DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake -DVCPKG_TARGET_TRIPLET=x64-windows-static
cmake --build build --config Release
```

Первая установка пакетов идёт долго (сборка из исходников, десятки минут) — это нормально.

### 2.3. Проверка

Признак успеха в логе — строка `SimpleKafka1C.vcxproj -> ...\build\Release\SimpleKafka1C.dll` и
отсутствие строк со словом `error`. Предупреждение
`LNK4070: /OUT:SampleAddIn.dll directive in .EXP differs...` — косметика из `addin.def`, игнорируется.

Версия внутри бинаря (Git Bash или Linux):
```bash
grep -a -o "1\.9\.2" build/Release/SimpleKafka1C.dll | head -1
```

---

## 3. Linux

### 3.1. Через Docker — рекомендуется, работает с любой ОС

```bash
scripts/build_linux.sh          # macOS / Linux / Git Bash
scripts\build_linux.bat         # cmd.exe
```

Скрипт собирает образ по `scripts/Dockerfile.ubuntu20` и вынимает библиотеку в
`build/linux/libSimpleKafka1C_Static.so`. Собранное на Ubuntu 20.04 работает на glibc >= 2.31.

Первый прогон долгий (vcpkg собирает зависимости), последующие — минуты: тяжёлые слои берутся из кэша,
пересобирается только слой проекта.

Версии зафиксированы в `Dockerfile.ubuntu20` аргументами `VCPKG_COMMIT` и `CMAKE_VERSION` — см. §5.8.
Разово проверить другой набор можно, не трогая файл:
```bash
docker build -f scripts/Dockerfile.ubuntu20 --build-arg VCPKG_COMMIT=<sha> -t kafka_1c_ubuntu .
```

> **Внутри образа файл называется `libSimpleKafka1C.so`** (без `_Static`), наружу копируется под именем
> `libSimpleKafka1C_Static.so`. Это исторические имена, а не признак разных сборок.

Если Docker Desktop не отвечает — запустить его и дождаться демона:
```bash
until docker info >/dev/null 2>&1; do sleep 5; done
```

### 3.2. Нативно на Linux

```bash
git clone https://github.com/microsoft/vcpkg ~/vcpkg && ~/vcpkg/bootstrap-vcpkg.sh
export VCPKG_OVERLAY_TRIPLETS="$PWD/scripts/triplets"     # триплет с -fPIC, см. §5.3
~/vcpkg/vcpkg install librdkafka avro-cpp protobuf abseil utf8-range \
    boost-property-tree boost-json boost-container snappy fmt curl --triplet x64-linux-static

cmake -B build -DCMAKE_TOOLCHAIN_FILE=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake \
      -DVCPKG_TARGET_TRIPLET=x64-linux-static -DCMAKE_BUILD_TYPE=Release
cmake --build build -j"$(nproc)"
```

Результат — `build/libSimpleKafka1C.so`.

---

## 4. Самопроверки

В `scripts/avro_selftest` лежит автономная программа, повторяющая путь декодирования компоненты без 1С.

```bash
cmake -B build/selftest -S scripts/avro_selftest -A x64 \
      -DCMAKE_TOOLCHAIN_FILE=C:/vcpkg/scripts/buildsystems/vcpkg.cmake \
      -DVCPKG_TARGET_TRIPLET=x64-windows-static      # на Linux: без -A x64 и со своим toolchain
cmake --build build/selftest --config Release
```

| Программа | Что проверяет | Запуск |
|---|---|---|
| `avro_selftest` | декодирование реальных сообщений указанной схемой — тем же путём, что и компонента | `avro_selftest <schema.json> <msg1.bin> [msg2.bin ...]` |

Файлы подаются декодеру как есть. base64 утилита **не распознаёт** — по той же причине, по которой
компонента требует явный параметр `ЭтоBase64` (см. [avro.md](avro.md)): сырой Avro-payload может
целиком состоять из символов алфавита base64, и угадывание превратило бы корректное сообщение в мусор.
Если сообщение пришло в base64, раскодируйте его до запуска утилиты.

---

## 5. Грабли и обоснования

### 5.1. avro-cpp обязан быть >= 1.12.1
На версиях ниже (воспроизведено на 1.11.3) декодирование глубоко вложенной рекурсивной схемы падает:
segfault либо `vector::_M_range_check ... >= size`; в 1С это выглядит как
`Error decoding AVRO: vector::_M_range_check`. Поэтому `scripts/build_windows.bat` и
`scripts/Dockerfile.ubuntu20` **валят сборку**, если vcpkg поставил версию ниже. Проверить, какая версия реально попала в бинарь, можно строкой внутри него:
`grep -a -o "1\.12\.[0-9]*" <бинарь>`.

### 5.2. Принудительное включение `fmt/format.h`
`<avro/Exception.hh>` вызывает `fmt::format()`. На связке avro-cpp 1.12.1 + fmt 12.1 всё собирается и без
дополнительных мер: `Exception.hh` подключает `<fmt/core.h>`, а тот тянет `format.h`. Но на других
сочетаниях версий в цепочку попадает только базовый заголовок fmt, и сборка падает на
`'format' is not a member of 'fmt'` — мы это ловили. Поэтому в `CMakeLists.txt` для каждого TU добавлено
`/FI fmt/format.h` (MSVC) и `-include fmt/format.h` (GCC/Clang): это снимает зависимость от того, какой
именно заголовок fmt подтянет конкретная версия avro-cpp, и ничего не ломает, когда проблемы нет.

### 5.3. Триплет `x64-linux-static` с `-fPIC`
Компонента — разделяемая библиотека, статически линкующая зависимости vcpkg. Статические архивы должны
быть position-independent, иначе финальная линковка `.so` падает. Готовый триплет —
`scripts/triplets/x64-linux-static.cmake`, подключается через `VCPKG_OVERLAY_TRIPLETS` (в Dockerfile
это уже сделано).

### 5.4. `boost-container`
`CMakeLists.txt` требует `find_package(Boost COMPONENTS json container)`. Без пакета `boost-container`
конфигурация CMake падает.

### 5.5. Статический CRT (`/MT`) на Windows
Триплет `x64-windows-static` собирает зависимости с `/MT`. Проект выставляет
`CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>"`; если это забыть в своём под-проекте
(например, в тестовой утилите), линковщик выдаст `LNK2038: mismatch detected for 'RuntimeLibrary'`.

### 5.6. Пути в `CMakeLists.txt` править не нужно
Пути к библиотекам приходят из `CMAKE_TOOLCHAIN_FILE` и `VCPKG_TARGET_TRIPLET`. Прописывать их руками
внутри файла проекта не требуется — иначе локальная правка попадает в коммиты и ломает сборку у других.

### 5.7. В Git Bash нет `zip`
Архив для 1С собирается PowerShell'ом (`Compress-Archive`) либо `zip` там, где он есть — см. §6.

### 5.8. Пины в `Dockerfile.ubuntu20`
Образ фиксирует две вещи, от которых зависит результат сборки:

| Аргумент | Значение | Зачем |
|---|---|---|
| `VCPKG_COMMIT` | `40f3c709db80acf154ac4b17a1f83c564ebd022e` (2026-07-24) | На этом коммите vcpkg ставит avro-cpp 1.12.1. Коммит задаёт версии **всех** зависимостей сразу |
| `CMAKE_VERSION` | `3.31.10` | В Ubuntu 20.04 встроен CMake 3.16, проекту нужен >= 3.20 |

Без пинов «воспроизводимая сборка» воспроизводит не сборку, а состояние интернета на день запуска:
`git clone` даёт текущий HEAD vcpkg, `pip3 install cmake --upgrade` — последний релиз CMake.
Версия avro-cpp при этом уезжает вместе с baseline — в том числе та самая, ради гейта которой всё
и делается. Отдельно про CMake: ветка 4.x снимает совместимость со старыми
`cmake_minimum_required`, и часть портов vcpkg на ней перестаёт конфигурироваться.

Обновление пина — осознанное действие: поменять значение в `Dockerfile.ubuntu20`, пересобрать образ,
убедиться, что гейт версии avro-cpp прошёл. Разово проверить другой набор можно через
`--build-arg`, не трогая файл (см. §3.1).

---

## 6. Упаковка в форму для 1С

Внешняя компонента для 1С — zip из трёх файлов **в корне архива**:

```
MANIFEST.XML
SimpleKafka1C64_<версия с подчёркиваниями>.dll
SimpleKafka1C64_<версия с подчёркиваниями>.so
```

`MANIFEST.XML` (схема — `include/MANIFEST.xsd`), пути = имена соседних файлов:

```xml
<?xml version="1.0" encoding="UTF-8" ?>
<bundle xmlns="http://v8.1c.ru/8.2/addin/bundle" name="SimpleKafka1C">
<component os="Windows" path="SimpleKafka1C64_1_9_2.dll" type="native" arch="x86_64" />
<component os="Linux"   path="SimpleKafka1C64_1_9_2.so"  type="native" arch="x86_64"/>
</bundle>
```

Сборка архива:

```powershell
# Windows PowerShell
$v = '1.9.2'; $vf = $v -replace '[.\-]','_'
$stage = "build\package\SimpleKafka_$v"
Remove-Item -Recurse -Force $stage -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force $stage | Out-Null
Copy-Item build\Release\SimpleKafka1C.dll "$stage\SimpleKafka1C64_$vf.dll"
Copy-Item build\linux\libSimpleKafka1C_Static.so "$stage\SimpleKafka1C64_$vf.so"
# рядом положить MANIFEST.XML с этими именами
Compress-Archive -Path "$stage\*" -DestinationPath "build\package\SimpleKafka_$v.zip" -Force
```

```bash
# macOS / Linux
v=1.9.2; vf=${v//[.-]/_}
stage="build/package/SimpleKafka_$v"; rm -rf "$stage"; mkdir -p "$stage"
cp build/Release/SimpleKafka1C.dll "$stage/SimpleKafka1C64_$vf.dll"
cp build/linux/libSimpleKafka1C_Static.so "$stage/SimpleKafka1C64_$vf.so"
(cd "$stage" && zip -q "../SimpleKafka_$v.zip" ./*)
```

**Оба бинаря обязательно собирать из одного коммита.** Разные версии dll и so в одном макете уже давали
трудноуловимую картину «на клиенте Windows работает, на сервере Linux падает».

Проверка архива:
```bash
unzip -l build/package/SimpleKafka_1.9.2.zip     # ровно 3 файла, без вложенных папок
unzip -p build/package/SimpleKafka_1.9.2.zip MANIFEST.XML
```

Подключение в 1С — [connection.md](connection.md). После обновления компоненты стоит убедиться, что
загрузилась ожидаемая версия: 1С кэширует внешние компоненты, и имя файла в макете может врать.

---

## Приложение. Сборка avro-cpp вручную

Нужна, если в vcpkg нет подходящего пакета.

```bash
wget https://dlcdn.apache.org/avro/avro-1.12.1/avro-src-1.12.1.tar.gz
tar -zxvf avro-src-1.12.1.tar.gz
# заменить avro/lang/c++/CMakeLists.txt файлом avro-cpp-linux/CMakeLists.txt из этого репозитория
cd avro-src-1.12.1/lang/c++
cmake -B . -S . -DCMAKE_TOOLCHAIN_FILE=<путь>/vcpkg/scripts/buildsystems/vcpkg.cmake -DVCPKG_TARGET_TRIPLET=x64-linux-static
make
```

Версия — строго 1.12.1 или новее, см. §5.1.
