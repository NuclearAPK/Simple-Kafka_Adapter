# Сборка внешней компоненты с использованием vcpkg
- Поставить https://github.com/microsoft/vcpkg по инструкции:
 ```
bootstrap-vcpkg
vcpkg integrate install
 ```

- Ставим пакеты Windows:
 ```
vcpkg install librdkafka --triplet x64-windows-static
vcpkg install avro-cpp --triplet x64-windows-static
vcpkg install boost-property-tree --triplet x64-windows-static
vcpkg install boost-json --triplet x64-windows-static
vcpkg install snappy --triplet x64-windows-static
 ```

- Ставим пакеты Linux:
 ```
vcpkg install librdkafka --triplet  x64-linux-static
vcpkg install boost-property-tree --triplet  x64-linux-static
vcpkg install boost-json --triplet  x64-linux-static
vcpkg install snappy --triplet  x64-linux-static
vcpkg install boost-filesystem --triplet  x64-linux-static
vcpkg install boost-iostreams --triplet  x64-linux-static
vcpkg install boost-program-options --triplet  x64-linux-static
vcpkg install boost-crc --triplet  x64-linux-static
vcpkg install boost-math --triplet  x64-linux-static
vcpkg install boost-format --triplet  x64-linux-static
vcpkg install boost-test --triplet  x64-linux-static
```

Сборку можно выполнить в Visual Studio как CMake проект.

# Linux

В версии для Linux необходимо avro-cpp собрать самостоятельно.
```
wget https://dlcdn.apache.org/avro/avro-1.12.0/avro-src-1.12.0.tar.gz
tar -zxvf avro-src-1.12.0.tar.gz
```
Файл **avro/lang/c++/CMakeLists.txt** необходимо заменить файлом **avro-cpp-linux/CMakeLists.txt** данного проекта. 
Сборка:
 ```
cd /home/source/avro-src-1.12.0/lang/c++
cmake -B . -S . -DCMAKE_TOOLCHAIN_FILE=/home/source/vcpkg/scripts/buildsystems/vcpkg.cmake -DVCPKG_TARGET_TRIPLET=x64-linux-static
make
```

В основном файле CMakeLists.txt проекта надо указать пути в **target_include_directories** и **target_link_libraries** до файлов проекта avro и vcpkg. 
В файле CMakeLists-linux.txt приведен пример файла CMakeLists.txt для Linux.

Далее сборка внешней компоненты:
 ```
cd /home/source/Simple-Kafka_Adapter
cmake -B . -S . -DCMAKE_TOOLCHAIN_FILE=/home/source/vcpkg/scripts/buildsystems/vcpkg.cmake -DVCPKG_TARGET_TRIPLET=x64-linux-static
make
```

Библиотека будет собрана для  x64-linux-static.



