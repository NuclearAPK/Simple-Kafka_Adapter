# Статическая линковка Protocol Buffers

Компонента настроена на статическую линковку всех зависимостей, включая Protocol Buffers. Это означает, что все необходимые библиотеки встроены в DLL/SO файл компоненты, и вам не нужно отдельно поставлять DLL protobuf.

## Как это работает

### CMakeLists.txt настройки:

1. **Использование статических библиотек из vcpkg:**
   ```cmake
   set(CMAKE_PREFIX_PATH "C:/Sources/vcpkg/installed/x64-windows-static")
   ```
   Путь указывает на **x64-windows-static** - это статические сборки библиотек.

2. **Флаги статической линковки:**
   ```cmake
   set(Protobuf_USE_STATIC_LIBS ON)
   add_definitions("-DPROTOBUF_USE_DLLS=0")
   ```

3. **Линковка зависимостей protobuf:**
   ```cmake
   target_link_libraries(${TARGET} PRIVATE protobuf::libprotobuf)
   target_link_libraries(${TARGET} PRIVATE utf8_range::utf8_validity)
   target_link_libraries(${TARGET} PRIVATE absl::strings absl::status absl::statusor)
   ```

### Необходимые пакеты через vcpkg:

**Windows:**
```bash
vcpkg install protobuf --triplet x64-windows-static
vcpkg install abseil --triplet x64-windows-static
vcpkg install utf8-range --triplet x64-windows-static
```

**Linux:**
```bash
vcpkg install protobuf --triplet x64-linux-static
vcpkg install abseil --triplet x64-linux-static
vcpkg install utf8-range --triplet x64-linux-static
```

## Проверка статической линковки

### Windows:
После сборки проверьте зависимости DLL:
```powershell
dumpbin /DEPENDENTS SimpleKafka1C.dll
```

Вы НЕ должны увидеть в списке:
- `libprotobuf.dll`
- `libabsl_*.dll`
- `utf8_range.dll`

### Linux:
```bash
ldd SimpleKafka1C.so | grep protobuf
```

Команда НЕ должна показывать libprotobuf.so в зависимостях.

## Размер компоненты

При статической линковке protobuf размер DLL/SO файла увеличивается на ~2-3 МБ, так как весь код protobuf встраивается в компоненту. Это нормально и является компромиссом за удобство развертывания.

## Преимущества статической линковки

1. **Простота развертывания** - нужен только один DLL/SO файл
2. **Нет конфликтов версий** - не нужно беспокоиться о версии protobuf на целевой системе
3. **Изолированность** - компонента полностью самодостаточна
4. **Совместимость** - работает на любой системе без дополнительных зависимостей

## Недостатки

1. **Размер файла** - компонента больше по размеру
2. **Память** - если на системе есть другие приложения с protobuf, код будет загружен дважды в память
3. **Обновления** - для обновления protobuf нужно пересобрать всю компоненту

## Альтернативный вариант (динамическая линковка)

Если по каким-то причинам нужна динамическая линковка, измените CMakeLists.txt:

```cmake
# Вместо x64-windows-static используйте x64-windows
set(CMAKE_PREFIX_PATH "C:/Sources/vcpkg/installed/x64-windows")

# Удалите или закомментируйте:
# set(Protobuf_USE_STATIC_LIBS ON)
# add_definitions("-DPROTOBUF_USE_DLLS=0")
```

Тогда потребуется поставлять libprotobuf.dll вместе с компонентой.
