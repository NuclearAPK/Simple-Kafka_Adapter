cmake_minimum_required(VERSION 3.20)

project(SimpleKafka1C LANGUAGES CXX)

# Allow local find modules for dependencies that ship without CMake configs.
list(PREPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# -----------------------------
# Options
# -----------------------------
option(CASE_INSENSITIVE "Case insensitive method names" OFF)
option(OUT_PARAMS        "Support output parameters" OFF)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Static CRT to match vcpkg x64-windows-static triplet (/MT)
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")

set(TARGET SimpleKafka1C)

# -----------------------------
# Sources
# -----------------------------
set(SOURCE_FILES
    src/addin.def
    src/stdafx.h
    src/dllmain.cpp
    src/exports.cpp
    src/md5.cpp
    src/md5.h
    src/Component.cpp
    src/Component.h
    src/SimpleKafka1C.cpp
    src/SimpleKafka1C.h
    src/utils.cpp
    src/utils.h
    src/producer_methods.cpp
    src/consumer_methods.cpp
    src/admin_topics.cpp
    src/admin_metadata.cpp
    src/admin_groups.cpp
    src/avro_methods.cpp
    src/schema_registry_methods.cpp
    src/protobuf_methods.cpp
)

add_library(${TARGET} SHARED ${SOURCE_FILES})

# -----------------------------
# Target compile definitions
# -----------------------------
target_compile_definitions(${TARGET} PRIVATE
    UNICODE
    _UNICODE
    LIBRDKAFKA_STATICLIB
    PROTOBUF_USE_DLLS=0
    LIBPROTOBUF_EXPORTS=
)

if (CASE_INSENSITIVE)
    target_compile_definitions(${TARGET} PRIVATE CASE_INSENSITIVE)
endif()

if (OUT_PARAMS)
    target_compile_definitions(${TARGET} PRIVATE OUT_PARAMS)
endif()

# -----------------------------
# Include dirs
# -----------------------------
target_include_directories(${TARGET} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# -----------------------------
# Platform-specific bits
# -----------------------------
if (WIN32)
    target_compile_definitions(${TARGET} PRIVATE
        WIN32
        _USRDLL
        _CRT_SECURE_NO_WARNINGS
        _CRT_SECURE_NO_DEPRECATE
        ADDINCPP_EXPORTS
        _WIN32_WINNT=0x0601
        _SILENCE_CXX17_CODECVT_HEADER_DEPRECATION_WARNING
    )
    if (MSVC)
        target_compile_options(${TARGET} PRIVATE /utf-8)
    endif()
else()
    target_compile_options(${TARGET} PRIVATE -Wall -Wextra)

    if (UNIX AND NOT APPLE)
        target_link_options(${TARGET} PRIVATE
            "LINKER:--no-undefined"
            "LINKER:--version-script=${CMAKE_CURRENT_SOURCE_DIR}/src/version.script"
        )
    endif()

    find_package(Threads REQUIRED)
    target_link_libraries(${TARGET} PRIVATE Threads::Threads)
endif()

# -----------------------------
# Dependencies (vcpkg-friendly)
# -----------------------------
set(Boost_USE_STATIC_LIBS ON)
set(Protobuf_USE_STATIC_LIBS ON)

find_package(RdKafka CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)
find_package(ZLIB REQUIRED)

# Workaround for avro-cpp config alias (some ports expect this target name)
if(NOT TARGET avro-cpp::avrocpp)
    add_library(avro-cpp::avrocpp INTERFACE IMPORTED)
endif()
find_package(avro-cpp CONFIG REQUIRED)

find_package(Boost REQUIRED COMPONENTS json container)

# Protobuf: runtime parsing only (no code generation needed).
# On Linux, protobuf config may reference absl/utf8_range targets explicitly.
if(UNIX AND NOT APPLE)
    find_package(absl CONFIG REQUIRED)
    find_package(utf8_range CONFIG REQUIRED)
endif()
# Work around a vcpkg protobuf export issue where protobuf::protoc is used in
# protobuf-targets-debug.cmake before it is declared, and then declared again.
if(UNIX AND NOT APPLE)
    if(NOT TARGET protobuf::protoc)
        add_executable(protobuf::protoc IMPORTED GLOBAL)
    endif()
    function(add_executable)
        if(ARGC GREATER 0 AND "${ARGV0}" STREQUAL "protobuf::protoc" AND TARGET protobuf::protoc)
            return()
        endif()
        _add_executable(${ARGV})
    endfunction()
endif()
find_package(Protobuf CONFIG REQUIRED)

find_package(CURL REQUIRED)

# OpenSSL integration for Linux TLS diagnostics and provider loading.
if(UNIX AND NOT APPLE)
    find_package(OpenSSL QUIET)
    if(OpenSSL_FOUND)
        target_include_directories(${TARGET} PRIVATE ${OPENSSL_INCLUDE_DIR})
        target_compile_definitions(${TARGET} PRIVATE SIMPLEKAFKA_HAS_OPENSSL=1)
        target_link_libraries(${TARGET} PRIVATE OpenSSL::Crypto)
    endif()
endif()

# -----------------------------
# Link libraries
# -----------------------------
if(TARGET avro-cpp::avrocpp_s)
    target_link_libraries(${TARGET} PRIVATE avro-cpp::avrocpp_s)
elseif(TARGET avro-cpp::avrocpp_static)
    target_link_libraries(${TARGET} PRIVATE avro-cpp::avrocpp_static)
elseif(TARGET avro-cpp::avrocpp)
    target_link_libraries(${TARGET} PRIVATE avro-cpp::avrocpp)
else()
    message(FATAL_ERROR "Could not find avro-cpp target")
endif()

target_link_libraries(${TARGET} PRIVATE
    fmt::fmt
    ZLIB::ZLIB
    RdKafka::rdkafka
    RdKafka::rdkafka++
    CURL::libcurl
    protobuf::libprotobuf
)

# Boost: keep compatibility with existing usage
target_link_libraries(${TARGET} PRIVATE ${Boost_LIBRARIES})

# Windows system libraries required by static OpenSSL / librdkafka
if(WIN32)
    target_link_libraries(${TARGET} PRIVATE bcrypt crypt32 ws2_32)
endif()

# Exclude dynamic abseil library to avoid conflicts with static protobuf (MSVC)
if(MSVC)
    target_link_options(${TARGET} PRIVATE "/NODEFAULTLIB:abseil_dll.lib")
endif()

# -----------------------------
# Linux SASL2 (if librdkafka is built with SASL support)
# -----------------------------
if (UNIX AND NOT APPLE)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(SASL2 REQUIRED libsasl2)
    target_include_directories(${TARGET} PRIVATE ${SASL2_INCLUDE_DIRS})
    target_link_libraries(${TARGET} PRIVATE ${SASL2_LIBRARIES})
endif()

# -----------------------------
# Diagnostics (optional)
# -----------------------------
message(STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
message(STATUS "VCPKG_TARGET_TRIPLET: ${VCPKG_TARGET_TRIPLET}")
