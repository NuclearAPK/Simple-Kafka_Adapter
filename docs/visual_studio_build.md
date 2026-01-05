# Сборка проекта через Visual Studio

## Важно!

Для правильной статической линковки protobuf необходимо открывать проект **как CMake проект**, а не как обычный .sln файл.

## Способ 1: Открыть CMake проект в Visual Studio (Рекомендуется)

1. Запустите Visual Studio 2026
2. Выберите **File → Open → CMake...**
3. Откройте файл `CMakeLists.txt` из корневой директории проекта
4. Visual Studio автоматически сконфигурирует проект с правильными настройками
5. В выпадающем списке конфигураций выберите **x64-Release**
6. Нажмите **Build → Build All** или `Ctrl+Shift+B`

## Способ 2: Сборка через командную строку с использованием MSBuild

```cmd
cd C:\Sources\Projects\Simple-Kafka_Adapter

REM Сгенерировать проект Visual Studio
cmake -B build -G "Visual Studio 18 2026" -A x64 ^
  -DCMAKE_TOOLCHAIN_FILE=C:/Sources/vcpkg/scripts/buildsystems/vcpkg.cmake ^
  -DVCPKG_TARGET_TRIPLET=x64-windows-static

REM Собрать проект
cmake --build build --config Release
```

## Проверка статической линковки

После сборки проверьте, что protobuf встроен статически:

```cmd
dumpbin /DEPENDENTS build\Release\SimpleKafka1C.dll
```

В списке зависимостей **НЕ должно быть**:
- `libprotobuf.dll`
- `libprotobuf-lite.dll`
- Любых других DLL protobuf

Должны быть **только системные библиотеки Windows**:
- bcrypt.dll
- ADVAPI32.dll
- CRYPT32.dll
- WS2_32.dll
- Secur32.dll
- KERNEL32.dll
- USER32.dll
- dbghelp.dll

## Важные настройки CMake

В `CMakeLists.txt` обязательно установлены следующие параметры для статической линковки:

```cmake
# Force static triplet for vcpkg
set(VCPKG_TARGET_TRIPLET "x64-windows-static" CACHE STRING "")

# Force protobuf to be statically linked
add_compile_definitions(PROTOBUF_USE_DLLS=0)
add_compile_definitions(LIBPROTOBUF_EXPORTS=)

# Use static libraries
set(Boost_USE_STATIC_LIBS ON)
set(protobuf_MSVC_STATIC_RUNTIME ON)
set(Protobuf_USE_STATIC_LIBS ON)
```

## Проблемы и решения

### Проблема: В DLL все еще есть зависимость от libprotobuf.dll

**Решение:**
1. Убедитесь, что используется triplet `x64-windows-static` из vcpkg
2. Удалите директорию `build` и пересоздайте проект
3. Проверьте, что в CMake выводе указаны статические библиотеки:
   ```
   Protobuf_LIBRARIES: C:/Sources/vcpkg/installed/x64-windows-static/lib/libprotobuf.lib
   ```

### Проблема: Visual Studio не находит CMake

**Решение:**
Установите компонент "C++ CMake tools for Windows" в Visual Studio Installer.

## Результат

После успешной сборки DLL будет находиться здесь:
- **Release**: `build/Release/SimpleKafka1C.dll`
- **Debug**: `build/Debug/SimpleKafka1C.dll`

DLL полностью самодостаточна и готова к использованию в 1С без дополнительных зависимостей!
