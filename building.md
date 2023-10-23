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
"x86-windows.cmake" с содержимым:
 ```
set(VCPKG_TARGET_ARCHITECTURE x86)
set(VCPKG_CRT_LINKAGE static)
set(VCPKG_LIBRARY_LINKAGE static)
 ```
"x64-linux.cmake" с содержимым:
 ```
set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE static)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)
 ```
"x86-linux.cmake" с содержимым:
 ```
set(VCPKG_TARGET_ARCHITECTURE x86)
set(VCPKG_CRT_LINKAGE static)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)
 ```
- Если ранее стоял lz4 (можно проверить командой vcpkg list), то удалить:
 ```
vcpkg remove lz4 --triplet x86-windows
vcpkg remove lz4 --triplet x64-windows
vcpkg remove lz4 --triplet x86-linux
vcpkg remove lz4 --triplet x64-linux
 ```
- Ставим пакеты. Важно! Необходимо иметь установленные системы сборки (например, Visual Studio C++ 2017 для сборки Windows версии или gcc для сборки linux версии) под ту ОС, которую устанавливаются пакеты.
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
vcpkg install avro-cpp --overlay-triplets=static-triplets --triplet x86-linux
vcpkg install avro-cpp --overlay-triplets=static-triplets --triplet x64-linux
	
vcpkg install boost-property-tree --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install boost-property-tree --overlay-triplets=static-triplets --triplet x64-windows
vcpkg install boost-property-tree --overlay-triplets=static-triplets --triplet x86-linux
vcpkg install boost-property-tree --overlay-triplets=static-triplets --triplet x64-linux
 ```
**В файле В CMakeLists.txt требуется поменять пути до пакетного менеджера vcpkg и библиотек.**

Дальнейшая сборка производится в штатной системе сборки, например, как CMake проект в Visual Studio C++.

# Специфичные для LINUX шаги

Установите libboost-all-dev обычным образом, к примеру:
 ```
sudo apt-get install libboost-all-dev
 ```
- Запустите cmake-gui.
- В поле sorcecode укажите путь к проекту Simple-Kafka_Adapter.
- В поле binaries укажите путь к папке в которую попадет созданный проект для сборки.
- Нажмите Configure.
- В открывшемся диалоге выберите тип проекта ninja и нажмите Finish.
- Далее, в появившемся списке переменных, исправьте пути до include и библиотек.
- Снова нажмите Configure.
- Далее нажмите Generate.
- Зайдите в консоли в папку, указанную в поле  binaries, и выполните команду ninja.
- Библиотека будет собрана.
