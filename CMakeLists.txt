cmake_minimum_required(VERSION 3.12)
project(SimpleKafka1C)

set(LIB_DIR ${CMAKE_SOURCE_DIR}/bin)
set(EXE_DIR ${CMAKE_SOURCE_DIR}/bin)
set(ARCH_DIR ${CMAKE_SOURCE_DIR}/bin)
set(PDB_DIR ${CMAKE_SOURCE_DIR}/debug_info)
set(PKG_OUTPUT_DIR ${CMAKE_SOURCE_DIR}/pkg)

file(MAKE_DIRECTORY ${LIB_DIR})
file(MAKE_DIRECTORY ${EXE_DIR})
file(MAKE_DIRECTORY ${ARCH_DIR})
file(MAKE_DIRECTORY ${PDB_DIR})
file(MAKE_DIRECTORY ${PKG_OUTPUT_DIR})

set (CMAKE_LIBRARY_OUTPUT_DIRECTORY ${LIB_DIR})
set (CMAKE_RUNTIME_OUTPUT_DIRECTORY ${EXE_DIR})
set (CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${ARCH_DIR})
set (CMAKE_PDB_OUTPUT_DIRECTORY ${PDB_DIR})

set(CMAKE_CXX_STANDARD 17)
set(TARGET SimpleKafka1C)

option(CASE_INSENSITIVE "Case insensitive method names" OFF)
option(STATIC_CRT "Static CRT linkage" ON)
option(OUT_PARAMS "Support output parameters" OFF)

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

add_definitions("-DLIBRDKAFKA_STATICLIB")

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
#adding library
if (WIN32)
#    add_definitions("-D_WIN32_WINNT=0x0601")
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
else()
    target_link_libraries(${TARGET} PRIVATE
        /home/shmell/libs/librdkafka++.a
        /home/shmell/libs/librdkafka.a
        /home/shmell/libs/libcrypto.a
        /home/shmell/libs/libcurl.a
        /home/shmell/libs/liblz4.a
        /home/shmell/libs/libssl.a
        /home/shmell/libs/libz.a
        /home/shmell/libs/libzstd.a) 
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

 #   set_target_properties(${TARGET} PROPERTIES LINK_FLAGS "/WHOLEARCHIVE")
endif ()

if (UNIX)
    if (TARGET_ARCH STREQUAL "x86")
        set(CMAKE_C_FLAGS "-m32 ${CMAKE_C_FLAGS}")
        set(CMAKE_CXX_FLAGS "-m32 ${CMAKE_CXX_FLAGS}")
    endif ()
#	set_target_properties(${TARGET} PROPERTIES LINK_FLAGS "-Wl,--whole-archive")
endif ()

if (ANDROID)
    if (CMAKE_BUILD_TYPE STREQUAL Release)
        add_custom_command(TARGET ${TARGET} POST_BUILD
                COMMAND ${CMAKE_STRIP} ${CMAKE_SHARED_LIBRARY_PREFIX}${TARGET}${CMAKE_SHARED_LIBRARY_SUFFIX})
    endif ()
endif ()

set(BOOST_ROOT "D:\\Source\\boost_1_68_0") 
set(Boost_USE_STATIC_LIBS ON) 
set(Boost_USE_MULTITHREADED OFF)  
set(Boost_USE_STATIC_RUNTIME ON) 
set(Boost_NO_SYSTEM_PATHS TRUE) 
find_package(Boost 1.68.0)
if(Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIRS})
    target_link_libraries(${TARGET} ${Boost_LIBRARIES})
endif()

find_package(RdKafka CONFIG REQUIRED)
if(RdKafka_FOUND)
    include_directories(${RdKafka_INCLUDE_DIRS})
	target_link_libraries(${TARGET} PRIVATE RdKafka::rdkafka RdKafka::rdkafka++)
endif()
