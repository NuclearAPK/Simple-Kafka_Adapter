cmake_minimum_required(VERSION 3.12)

# Force static triplet for vcpkg (important for Visual Studio builds)
if (WIN32)
    set(VCPKG_TARGET_TRIPLET "x64-windows-static" CACHE STRING "")
    set(CMAKE_PREFIX_PATH "C:/Sources/vcpkg/installed/x64-windows-static")
else()
    set(VCPKG_TARGET_TRIPLET "x64-linux-static" CACHE STRING "")
    set(CMAKE_PREFIX_PATH "/usr/local/lib/pkgconfig/"
    "/home/shmell/vcpkg/installed/x64-linux-release/")
endif ()

# Force static linking (before project())
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>" CACHE STRING "")

project(SimpleKafka1C)

set(CMAKE_CXX_STANDARD 17)
set(TARGET SimpleKafka1C)

option(CASE_INSENSITIVE "Case insensitive method names" OFF)
option(STATIC_CRT "Static CRT linkage" ON)
option(OUT_PARAMS "Support output parameters" OFF)

list(APPEND SOURCE_FILES
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

add_library(${TARGET} SHARED ${SOURCE_FILES})

if (WIN32)
    add_definitions(-DUNICODE -DWIN32 -D_USRDLL -D_CRT_SECURE_NO_WARNINGS -D_CRT_SECURE_NO_DEPRECATE -DADDINCPP_EXPORTS)
    IF(TARGET_PLATFORM_32)
        add_definitions(-D_USE_32BIT_TIME_T )
    ENDIF()
endif()
IF (UNIX AND NOT APPLE)
    set_target_properties(SimpleKafka1C PROPERTIES LINK_FLAGS "-Wl,--no-undefined -Xlinker --version-script -Xlinker /home/shmell/projects/Simple-Kafka_Adapter-main/src/version.script") 
ENDIF()

add_definitions("-DLIBRDKAFKA_STATICLIB")
add_definitions("-DPROTOBUF_USE_DLLS=0")

target_compile_definitions(${TARGET} PRIVATE
        UNICODE
        _UNICODE
        PROTOBUF_USE_DLLS=0)

if (CASE_INSENSITIVE)
    target_compile_definitions(${TARGET} PRIVATE CASE_INSENSITIVE)
endif ()

if (OUT_PARAMS)
    target_compile_definitions(${TARGET} PRIVATE OUT_PARAMS)
endif ()

#adding library
set(Boost_USE_STATIC_LIBS ON)
set(protobuf_MSVC_STATIC_RUNTIME ON)
set(Protobuf_USE_STATIC_LIBS ON)

find_package(RdKafka CONFIG REQUIRED)
find_package(unofficial-avro-cpp CONFIG REQUIRED)
find_package(Boost REQUIRED COMPONENTS json container)
find_package(Protobuf CONFIG REQUIRED)
find_package(absl CONFIG REQUIRED)
find_package(utf8_range CONFIG REQUIRED)
find_package(CURL REQUIRED)

# Print debug info
message(STATUS "Protobuf_LIBRARIES: ${Protobuf_LIBRARIES}")
message(STATUS "Protobuf_INCLUDE_DIRS: ${Protobuf_INCLUDE_DIRS}")

# Force protobuf to be statically linked (critical for Visual Studio)
add_compile_definitions(PROTOBUF_USE_DLLS=0)
add_compile_definitions(LIBPROTOBUF_EXPORTS=)
if (MSVC)
    # Remove any dynamic linking flags that might be set
    string(REPLACE "PROTOBUF_USE_DLLS=1" "PROTOBUF_USE_DLLS=0" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
    string(REPLACE "/DPROTOBUF_USE_DLLS" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
endif()

target_link_libraries(${TARGET} PRIVATE unofficial::avro-cpp::avrocpp)
target_link_libraries(${TARGET} PRIVATE ${Boost_LIBRARIES})
target_link_libraries(${TARGET} PRIVATE RdKafka::rdkafka RdKafka::rdkafka++)
target_link_libraries(${TARGET} PRIVATE CURL::libcurl)

# Link protobuf and all its dependencies statically
target_link_libraries(${TARGET} PRIVATE
    protobuf::libprotobuf
    protobuf::libprotobuf-lite)
target_link_libraries(${TARGET} PRIVATE utf8_range::utf8_validity utf8_range::utf8_range)
target_link_libraries(${TARGET} PRIVATE
    absl::strings
    absl::string_view
    absl::str_format
    absl::cord
    absl::status
    absl::statusor
    absl::log_internal_message
    absl::log_internal_check_op
    absl::synchronization
    absl::stacktrace
    absl::symbolize
    absl::debugging_internal
    absl::demangle_internal
    absl::malloc_internal
    absl::time
    absl::civil_time
    absl::time_zone
    absl::bad_optional_access
    absl::bad_variant_access
    absl::base
    absl::throw_delegate
    absl::raw_logging_internal
    absl::log_severity
    absl::spinlock_wait
)

target_include_directories(${TARGET} PRIVATE include ${SOURCES}/include)

if (WIN32)
    add_definitions("-D_WIN32_WINNT=0x0601")
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)

    if("${PORT}" STREQUAL "snappy")
        list(APPEND VCPKG_CMAKE_CONFIGURE_OPTIONS -DSNAPPY_HAVE_SSSE3=ON -DSNAPPY_HAVE_BMI2=ON)
    endif()
else()
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(SASL2 REQUIRED libsasl2)

    target_link_libraries(${TARGET} PRIVATE rdkafka ${SASL2_LIBRARIES})
    target_include_directories(${TARGET} PRIVATE ${SASL2_INCLUDE_DIRS})
    target_link_libraries(${TARGET} PRIVATE pthread)
endif ()

if (WIN32 AND NOT MSVC)
    message(FATAL_ERROR "Must be compiled with MSVC on Windows")
endif ()

if (WIN32)
    if (STATIC_CRT)
        string(REPLACE "/MD" "/MT" CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
        string(REPLACE "/MD" "/MT" CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
    endif ()
    target_compile_definitions(${TARGET} PRIVATE
            _WINDOWS
            _SILENCE_CXX17_CODECVT_HEADER_DEPRECATION_WARNING)
    target_compile_options(${TARGET} PRIVATE /utf-8)
else ()
    if (CMAKE_COMPILER_IS_GNUCXX)
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -flto -O3 -DNDEBUG -s")
    endif ()
endif ()
