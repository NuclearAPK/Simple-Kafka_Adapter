# Сборка внешней компоненты с помощью Visual Studio 2017
- устновить boost версии - 1.68, так как следующая уже не поддерживается VS2017
- установить boost, например,  с https://sourceforge.net/projects/boost/. Установка - bootstrap.bat, потом b2.exe
- поставить https://github.com/microsoft/vcpkg. По инструкции:
 ```
 bootstrap-vcpkg.bat
vcpkg integrate install
 ```
В CMakeSettings.json прописан vcpkg и триплеты.
- создать папку ".\vcpkg\static-triplets" в которой создать файлы "x64-windows.cmake" с содержимым:
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
vcpkg install librdkafka  --overlay-triplets=static-triplets --triplet x86-windows
vcpkg install lz4  --overlay-triplets=static-triplets --triplet x64-windows
vcpkg install librdkafka  --overlay-triplets=static-triplets --triplet x64-windows
 ```

