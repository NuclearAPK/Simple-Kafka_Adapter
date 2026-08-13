REM Запускать из корня репозитория в "x64 Native Tools Command Prompt for VS"
REM (нужны cl.exe и cmake из MSVC). boost-container обязателен: его требует
REM find_package(Boost COMPONENTS json container) в CMakeLists.txt.

REM Берём уже установленный vcpkg из %VCPKG_ROOT%, и только если переменной нет —
REM работаем с C:\vcpkg. Иначе на машине, где vcpkg лежит в другом каталоге,
REM скрипт создаёт второй экземпляр и пересобирает все зависимости с нуля.
if not defined VCPKG_ROOT set "VCPKG_ROOT=C:\vcpkg"
if not exist "%VCPKG_ROOT%\vcpkg.exe" (
    if not exist "%VCPKG_ROOT%" git clone https://github.com/microsoft/vcpkg "%VCPKG_ROOT%" || exit /b 1
    call "%VCPKG_ROOT%\bootstrap-vcpkg.bat" || exit /b 1
)
echo vcpkg: %VCPKG_ROOT%

REM `vcpkg integrate install` сознательно не вызывается: он прописывает user-wide
REM MSBuild props и меняет поведение всех проектов на машине, а сборке через
REM CMAKE_TOOLCHAIN_FILE не нужен.
"%VCPKG_ROOT%\vcpkg" install librdkafka avro-cpp protobuf abseil utf8-range boost-property-tree boost-json boost-container snappy fmt curl --triplet x64-windows-static || exit /b 1
REM ВАЖНО: avro-cpp ОБЯЗАН быть >= 1.12.1. В версиях ниже (подтверждено на 1.11.3) баг декодирования
REM глубоко-вложенных рекурсивных Avro-схем (segfault / vector range_check на проде). В 1.12.1 исправлено.
REM Берём только строку самого порта: `vcpkg list avro-cpp` печатает ещё и строки
REM установленных фич (`avro-cpp[snappy]:...  Support Snappy for compression`),
REM у которых во второй колонке не версия. Цикл присваивает переменную на каждой
REM итерации, поэтому без фильтра по началу строки в AVROVER оказывалось "Support"
REM и гейт валил сборку даже на корректной 1.12.1.
REM Список пишем во временный файл: `for /f` запускает команду через `cmd /c`,
REM и путь к vcpkg в кавычках вместе с конвейером даёт "The filename, directory
REM name, or volume label syntax is incorrect". Через файл кавычки в пути безопасны.
"%VCPKG_ROOT%\vcpkg" list avro-cpp > "%TEMP%\skafka_avro_list.txt"
for /f "tokens=2" %%v in ('findstr /b /c:"avro-cpp:" "%TEMP%\skafka_avro_list.txt"') do set "AVROVER=%%v"
del "%TEMP%\skafka_avro_list.txt"
echo avro-cpp version: %AVROVER%
echo %AVROVER% | findstr /b /c:"1.12.1" /c:"1.12.2" /c:"1.12.3" /c:"1.12.4" /c:"1.12.5" /c:"1.12.6" /c:"1.12.7" /c:"1.12.8" /c:"1.12.9" /c:"1.13." /c:"1.14." /c:"1.15." /c:"2." >nul || (echo ERROR: avro-cpp %AVROVER% recursive-schema decode bug, need ^>= 1.12.1 & exit /b 1)
REM Генератор не фиксируем — CMake сам подберёт установленную Visual Studio.
cmake -B build -A x64 -DCMAKE_TOOLCHAIN_FILE="%VCPKG_ROOT%\scripts\buildsystems\vcpkg.cmake" -DVCPKG_TARGET_TRIPLET=x64-windows-static || exit /b 1
cmake --build build --config Release
