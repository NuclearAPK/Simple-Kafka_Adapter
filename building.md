# Сборка внешней компоненты с использованием vcpkg
- Поставить https://github.com/microsoft/vcpkg по инструкции:
 ```
bootstrap-vcpkg
vcpkg integrate install
 ```
- В файле CMakeLists.json указан путь до vcpkg.
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
 ```
и "x86-linux.cmake" с содержимым:
 ```
set(VCPKG_TARGET_ARCHITECTURE x86)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
 ```
- Ставим пакеты:
 ```

vcpkg install librdkafka --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install librdkafka --overlay-triplets=static-triplets --triplet x64-windows
vcpkg install librdkafka --overlay-triplets=static-triplets --triplet x86-linux
vcpkg install librdkafka --overlay-triplets=static-triplets --triplet x64-linux

vcpkg install avro-cpp --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install avro-cpp --overlay-triplets=static-triplets --triplet x64-windows
vcpkg install avro-cpp --overlay-triplets=static-triplets --triplet x86-linux
vcpkg install avro-cpp --overlay-triplets=static-triplets --triplet x64-linux

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
