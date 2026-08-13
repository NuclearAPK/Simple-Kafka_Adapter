# scripts — сборка компоненты

Все скрипты запускаются **из корня репозитория** (внутри относительные пути).
Полная инструкция со всеми граблями — [docs/building.md](../docs/building.md).

| Файл | Назначение |
|---|---|
| `build_windows.bat` | Windows: vcpkg + зависимости + гейт версии avro-cpp + Release-сборка → `build\Release\SimpleKafka1C.dll` |
| `build_linux.sh` / `build_linux.bat` | Linux-бинарь через Docker (образ по `Dockerfile.ubuntu20`) → `build/linux/libSimpleKafka1C_Static.so` |
| `Dockerfile.ubuntu20` | Образ сборки на Ubuntu 20.04 (glibc >= 2.31). Гейтит avro-cpp >= 1.12.1 |
| `triplets/x64-linux-static.cmake` | Overlay-триплет vcpkg с `-fPIC` — без него `.so` не линкуется |
| `avro_selftest/` | Автономная проверка декодирования Avro без 1С |

Перед пересборкой Windows «с нуля» каталог `build` удаляют — иначе кэш CMake тянет прежние настройки.
