cmake_minimum_required(VERSION 3.12)
project(SimpleKafka1C)

set(CMAKE_CXX_STANDARD 17)
set(TARGET SimpleKafka1C)

set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)

if (UNIX)
	list(APPEND SOURCES src)
else ()
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
endif ()

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