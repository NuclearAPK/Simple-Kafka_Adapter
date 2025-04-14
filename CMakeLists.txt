cmake_minimum_required(VERSION 3.12)

if (WIN32)
    set(CMAKE_PREFIX_PATH "D:/Libs/vcpkg/installed/x64-windows-static") 
else()    
    set(CMAKE_PREFIX_PATH "/usr/local/lib/pkgconfig/"
    "/home/shmell/vcpkg/installed/x64-linux/") 
endif ()

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
    set_target_properties(SimpleKafka1C PROPERTIES LINK_FLAGS "-Wl,--no-undefined -Xlinker --version-script -Xlinker src/version.script") 
ENDIF()

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

#adding library
if (WIN32)

    set(Boost_USE_STATIC_LIBS ON)

    find_package(RdKafka CONFIG REQUIRED)
    find_package(unofficial-avro-cpp CONFIG REQUIRED)
    find_package(Boost REQUIRED COMPONENTS json container)
    target_link_libraries(${TARGET} PRIVATE RdKafka::rdkafka RdKafka::rdkafka++)
    target_link_libraries(${TARGET} PRIVATE unofficial::avro-cpp::avrocpp) 
    target_link_libraries(${TARGET} PRIVATE ${Boost_LIBRARIES})

    target_include_directories(${TARGET} PRIVATE include ${SOURCES}/include)
        
    add_definitions("-D_WIN32_WINNT=0x0601")
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)

    if("${PORT}" STREQUAL "snappy")
        list(APPEND VCPKG_CMAKE_CONFIGURE_OPTIONS -DSNAPPY_HAVE_SSSE3=ON -DSNAPPY_HAVE_BMI2=ON)
    endif()
else()
    find_package(PkgConfig REQUIRED) 
    target_link_libraries(${TARGET} PRIVATE
    /usr/local/lib/librdkafka++.a
    /usr/local/lib/librdkafka-static.a
	/usr/local/lib/libavro.a
    pthread)  
    target_include_directories(${TARGET} PRIVATE
        include /home/shmell/Source/include)
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