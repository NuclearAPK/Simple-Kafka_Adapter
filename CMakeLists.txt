cmake_minimum_required(VERSION 3.12)
project(SimpleKafka1C)

set(CMAKE_CXX_STANDARD 17)
set(TARGET SimpleKafka1C)

set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

if (UNIX)
	option(CASE_INSENSITIVE "Case insensitive method names" OFF)
	option(OUT_PARAMS "Support output parameters" OFF)
	set(ZLIB_INCLUDE_DIR "/home/source/vcpkg/microsoft/vcpkg/packages/zlib_x64-linux/include" CACHE STRING "zlib include dir")
	set(RdKafka_DIR "/home/source/vcpkg/microsoft/vcpkg/packages/librdkafka_x64-linux/share/RdKafka" CACHE STRING "rdkafka cmake config")
	set(LZ4_DIR "/home/source/vcpkg/microsoft/vcpkg/packages/lz4_x64-linux/share/lz4" CACHE STRING "LZ4 cmake config")
	set(unofficial-avro-cpp_DIR "/home/source/vcpkg/microsoft/vcpkg/packages/avro-cpp_x64-linux/share/unofficial-avro-cpp" CACHE STRING "avro-cpp cmake config")
	set(unofficial-avro-cpp_INCLUDE_DIRS "/home/source/vcpkg/microsoft/vcpkg/packages/avro-cpp_x64-linux/include" CACHE STRING "avro-cpp include dir")
	set(ZLIB_LIBRARY "/home/source/vcpkg/microsoft/vcpkg/packages/zlib_x64-linux/lib/libz.a" CACHE STRING "zlib library file path")
	set(VCPKG_TARGET_TRIPLET "x64-linux" CACHE STRING "vcpkg triplet")
endif ()


list(APPEND SOURCES
        src/addin.def
        src/stdafx.h
        src/dllmain.cpp
        src/exports.cpp
        src/md5.cpp
        src/md5.h
        src/Component.cpp
        src/Component.h
        src/SimpleKafka1C.cpp
        src/SimpleKafka1C.h)

if (ANDROID)
    list(APPEND SOURCES
            src/jnienv.cpp
            src/jnienv.h)
endif ()

add_library(${TARGET} SHARED
        ${SOURCES})

target_compile_definitions(${TARGET} PRIVATE
        UNICODE
        _UNICODE)

if (CASE_INSENSITIVE)
    target_compile_definitions(${TARGET} PRIVATE CASE_INSENSITIVE)
endif ()

if (OUT_PARAMS)
    target_compile_definitions(${TARGET} PRIVATE OUT_PARAMS)
endif ()

target_include_directories(${TARGET} PRIVATE include ${SOURCES}/include)

if (WIN32)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(${TARGET} PRIVATE
            _WINDOWS
            _SILENCE_CXX17_CODECVT_HEADER_DEPRECATION_WARNING)
    target_compile_options(${TARGET} PRIVATE /utf-8)
endif ()

if (WIN32 AND NOT MSVC)
    message(FATAL_ERROR "Must be compiled with MSVC on Windows")
endif ()

if (UNIX)
    if (TARGET_ARCH STREQUAL "x86")
        set(CMAKE_C_FLAGS "-m32 ${CMAKE_C_FLAGS}")
        set(CMAKE_CXX_FLAGS "-m32 ${CMAKE_CXX_FLAGS}")
    endif ()
endif ()

if (ANDROID)
    if (CMAKE_BUILD_TYPE STREQUAL Release)
        add_custom_command(TARGET ${TARGET} POST_BUILD
                COMMAND ${CMAKE_STRIP} ${CMAKE_SHARED_LIBRARY_PREFIX}${TARGET}${CMAKE_SHARED_LIBRARY_SUFFIX})
    endif ()
endif ()

set(Boost_USE_MULTITHREADED OFF)  
find_package(Boost)
if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(${TARGET} ${Boost_LIBRARIES})
endif()

add_definitions("-DLIBRDKAFKA_STATICLIB")

find_package(RdKafka CONFIG REQUIRED)
if(RdKafka_FOUND)
    include_directories(${RdKafka_INCLUDE_DIRS})
	target_link_libraries(${TARGET} PRIVATE RdKafka::rdkafka RdKafka::rdkafka++)
endif()

find_package(unofficial-avro-cpp CONFIG REQUIRED)
if(unofficial-avro-cpp_FOUND)
    include_directories(${unofficial-avro-cpp_INCLUDE_DIRS})
	target_link_libraries(${TARGET} PRIVATE unofficial::avro-cpp::avrocpp)
endif()

find_package(nlohmann_json CONFIG REQUIRED)
if(nlohmann_json_FOUND)
    include_directories(${nlohmann_json_INCLUDE_DIRS})
	target_link_libraries(${TARGET} PRIVATE nlohmann_json::nlohmann_json)
endif()