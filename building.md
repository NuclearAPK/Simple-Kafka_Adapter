# Сборка внешней компоненты с использованием vcpkg
- Поставить https://github.com/microsoft/vcpkg по инструкции:
 ```
bootstrap-vcpkg
vcpkg integrate install
 ```
- В файле CMakeLists.json указан путь до vcpkg. Этот файл для Visual Studio.
- Создать папку ".\vcpkg\static-triplets" на одном уровне с папкой "triplets" в которой создать файл "x64-windows.cmake" с содержимым:
 ```
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
 ```
"x86-windows.cmake" с содержимым:
 ```
set(VCPKG_TARGET_ARCHITECTURE x86)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
 ```
"x64-linux.cmake"
 ```
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)
 ```
- Ставим пакеты:
 ```

vcpkg install librdkafka --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install librdkafka --overlay-triplets=static-triplets --triplet x64-windows
vcpkg install librdkafka --overlay-triplets=static-triplets --triplet x86-linux
vcpkg install librdkafka --overlay-triplets=static-triplets --triplet x64-linux

vcpkg install avro-cpp --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install avro-cpp --overlay-triplets=static-triplets --triplet x64-windows

vcpkg install boost-test --overlay-triplets=static-triplets --triplet x86-linux
vcpkg install boost-test --overlay-triplets=static-triplets --triplet x64-linux

vcpkg install boost-property-tree --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install boost-property-tree --overlay-triplets=static-triplets --triplet x64-windows
vcpkg install boost-property-tree --overlay-triplets=static-triplets --triplet x86-linux
vcpkg install boost-property-tree --overlay-triplets=static-triplets --triplet x64-linux

vcpkg install boost-json --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install boost-json --overlay-triplets=static-triplets --triplet x64-windows
vcpkg install boost-json --overlay-triplets=static-triplets --triplet x86-linux
vcpkg install boost-json --overlay-triplets=static-triplets --triplet x64-linux

vcpkg install snappy --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install snappy --overlay-triplets=static-triplets --triplet x64-windows
vcpkg install snappy --overlay-triplets=static-triplets --triplet x86-linux
vcpkg install snappy --overlay-triplets=static-triplets --triplet x64-linux
 ```

Сборку можно выполнить в Visual Studio как CMake проект или же командами

# Linux

В версии для Linux необходимо avro-cpp собрать самостоятельно.
```
wget https://dlcdn.apache.org/avro/avro-1.11.3/avro-src-1.11.3.tar.gz
tar -zxvf avro-src-1.11.3.tar.gz
```
Файл **avro/lang/c++/CMakeLists.txt** необходимо заменить файлом **avro-cpp-linux/CMakeLists.txt** данного проекта. В файле необходимо указать путь до **libz.a** из vcpkg. Сборка:
 ```
cd /home/source/avro-src-1.11.3/lang/c++
cmake -B . -S . -DCMAKE_TOOLCHAIN_FILE=/home/source/vcpkg/scripts/buildsystems/vcpkg.cmake -DVCPKG_TARGET_TRIPLET=x64-linux
make
```

В основном файле CMakeLists.txt проекта надо указать пути в **target_include_directories** и **target_link_libraries** до файлов проекта avro и vcpkg.

Далее сборка внешней компоненты:
 ```
 cd /home/source/Simple-Kafka_Adapter
cmake -B . -S . -DCMAKE_TOOLCHAIN_FILE=/home/source/vcpkg/scripts/buildsystems/vcpkg.cmake -DVCPKG_TARGET_TRIPLET=x64-linux
make
```

Библиотека будет собрана для x64-linux.
