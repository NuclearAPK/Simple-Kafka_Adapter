# Сборка внешней компоненты с использованием vcpkg
- Поставить https://github.com/microsoft/vcpkg по инструкции:
 ```
bootstrap-vcpkg
vcpkg integrate install
 ```
- Создать папку ".\vcpkg\static-triplets" на одном уровне с папкой "triplets" в которой создать файл "x64-windows.cmake" с содержимым:
 ```
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE static)
set(VCPKG_LIBRARY_LINKAGE static)
 ```
и "x86-windows.cmake" с содержимым:
 ```
set(VCPKG_TARGET_ARCHITECTURE x86)
set(VCPKG_CRT_LINKAGE static)
set(VCPKG_LIBRARY_LINKAGE static)
 ```
- Если ранее стоял lz4 (можно проверить командой vcpkg list), то удалить:
 ```
vcpkg remove lz4 --triplet x86-windows
vcpkg remove lz4 --triplet x64-windows
 ```
- Ставим пакеты:
 ```
vcpkg install lz4  --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install lz4  --overlay-triplets=static-triplets --triplet x64-windows
vcpkg install lz4  --overlay-triplets=static-triplets --triplet x86-linux
vcpkg install lz4  --overlay-triplets=static-triplets --triplet x64-linux

vcpkg install librdkafka  --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install librdkafka  --overlay-triplets=static-triplets --triplet x64-windows
vcpkg install librdkafka  --overlay-triplets=static-triplets --triplet x86-linux
vcpkg install librdkafka  --overlay-triplets=static-triplets --triplet x64-linux

vcpkg install avro-cpp --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install avro-cpp --overlay-triplets=static-triplets --triplet x64-windows
	
vcpkg install boost-property-tree --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install boost-property-tree --overlay-triplets=static-triplets --triplet x64-windows
vcpkg install boost-property-tree --overlay-triplets=static-triplets --triplet x86-linux
vcpkg install boost-property-tree --overlay-triplets=static-triplets --triplet x64-linux

vcpkg install nlohmann-json --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install nlohmann-json --overlay-triplets=static-triplets --triplet x64-windows
vcpkg install nlohmann-json --overlay-triplets=static-triplets --triplet x86-linux
vcpkg install nlohmann-json --overlay-triplets=static-triplets --triplet x64-linux
 ```

Сборку можно выполнить в Visual Studio как CMake проект.

# LINUX
Установите libboost-all-dev обычным образом, к примеру
 ```
sudo apt-get install libboost-all-dev
 ```

avro-cpp (https://github.com/confluentinc/avro-cpp-packaging) необходимо собрать самостоятельно с указанием set(CMAKE_CXX_STANDARD 17) в CMakeLists.txt. Версия 1.11.3 в vcpkg для linux собрана с опицей использования 11 версии стандарта, что вызывает конфликт времени выполнения (segfault).

- Запустите cmake-gui. 
- В поле sorcecode укажите путь к проекту Simple-Kafka_Adapter. 
- В поле binaries укажите путь к папке в которую попадет созданный проект для сборки.
- Нажмите Configure. 
- В открывшемся диалоге выберите тип проекта ninja и нажмите Finish. 
- В появившемся списке переменных, исправьте пути до include и библиотек. Для переменной RdKafka_DIR путь до папки с файлом cmake "путь к vcpkg/microsoft/vcpkg/packages/librdkafka_x64-linux/share/RdKafka". Аналогично переменной LZ4_DIR "путь к vcpkg/microsoft/vcpkg/packages/lz4_x64-linux/share/lz4". Переменной unofficial-avro-cpp_DIR "...vcpkg/microsoft/vcpkg/packages/avro-cpp_x64-linux/share/unofficial-avro-cpp"
- Снова нажмите Configure. 
- Далее нажмите Generate. 
- Зайдите в консоли в папку, указанную в поле  binaries, и выполните команду ninja. 
- Если возникла ошибка "ninja: error: '...vcpkg/microsoft/vcpkg/packages/avro-cpp_x64-linux/lib/libboost_filesystem.a', needed by 'libSimpleKafka1C.so', missing and no known rule to make it" Скопируйте эту и возможно некоторые другие библиотеки в указанную в сообщении папку и повторите сборку командой ninja.

Библиотека будет собрана.
