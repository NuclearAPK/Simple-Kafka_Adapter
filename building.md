# Сборка внешней компоненты с помощью Visual Studio 2017
- поставить https://github.com/microsoft/vcpkg по инструкции:
 ```
bootstrap-vcpkg.bat
vcpkg integrate install
 ```
В CMakeSettings.json проекта SimpleKafka1C прописан vcpkg и триплеты.
- создать папку ".\vcpkg\static-triplets" в которой создать файл "x64-windows.cmake" с содержимым:
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
- Если ранее стоял lz4, то удалить
 ```
vcpkg remove lz4 --triplet x86-windows
vcpkg remove lz4 --triplet x64-windows
 ```
- Ставим пакеты:
 ```
vcpkg install lz4  --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install lz4  --overlay-triplets=static-triplets --triplet x64-windows

vcpkg install librdkafka  --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install librdkafka  --overlay-triplets=static-triplets --triplet x64-windows

vcpkg install avro-cpp --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install avro-cpp --overlay-triplets=static-triplets --triplet x64-windows
	
vcpkg install boost-property-tree --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install boost-property-tree --overlay-triplets=static-triplets --triplet x64-windows
 ```

