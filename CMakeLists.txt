# CMakeList.txt : Top-level CMake project file, do global configuration
# and include sub-projects here.
#
cmake_minimum_required (VERSION 3.12)

set(CMAKE_CXX_STANDARD 17)

project (SimpleKafka1C)

find_package(Boost 1.38 REQUIRED COMPONENTS filesystem iostreams program_options regex system json)

find_package(Snappy)
if (SNAPPY_FOUND)
    set(SNAPPY_PKG libsnappy)
    add_definitions(-DSNAPPY_CODEC_AVAILABLE)
    message("Enabled snappy codec")
endif (SNAPPY_FOUND)

find_package(RdKafka CONFIG REQUIRED)

add_definitions (${Boost_LIB_DIAGNOSTIC_DEFINITIONS})
add_definitions("-DLIBRDKAFKA_STATICLIB")

if (CMAKE_COMPILER_IS_GNUCXX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -flto -O3 -DNDEBUG -s")
endif ()

set(SOURCE_FILES
    src/stdafx.h
    src/exports.cpp
    src/md5.cpp
    src/md5.h
    src/Component.cpp
    src/Component.h
    src/SimpleKafka1C.cpp
    src/SimpleKafka1C.h)

add_library(SimpleKafka1C SHARED ${SOURCE_FILES})

if (WIN32)
	find_package(unofficial-avro-cpp CONFIG REQUIRED)
	target_include_directories(SimpleKafka1C PRIVATE include ${SOURCES}/include 
		${Boost_INCLUDE_DIRS} 
		${RdKafka_INCLUDE_DIRS} 
		${unofficial-avro-cpp_INCLUDE_DIRS})
	target_link_libraries(SimpleKafka1C ${Boost_LIBRARIES}
		${SNAPPY_LIBRARIES} 
		RdKafka::rdkafka RdKafka::rdkafka++
		unofficial::avro-cpp::avrocpp)
else()
	target_include_directories(SimpleKafka1C PRIVATE include ${SOURCES}/include 
		${Boost_INCLUDE_DIRS} 
		${RdKafka_INCLUDE_DIRS} 
		/home/gaid/source/avro-src-1.12.0/lang/c++/api)
	target_link_libraries(SimpleKafka1C ${Boost_LIBRARIES}
		${SNAPPY_LIBRARIES} 
		/home/gaid/source/avro-src-1.12.0/lang/c++/libavrocpp_s.a
		/home/gaid/source/vcpkg/installed/x64-linux/lib/libboost_iostreams.a
		/home/gaid/source/vcpkg/installed/x64-linux/lib/libsnappy.a
		/home/gaid/source/vcpkg/installed/x64-linux/lib/libz.a
		RdKafka::rdkafka RdKafka::rdkafka++)
endif ()

if (WIN32)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS)
    target_compile_definitions(SimpleKafka1C PRIVATE
            _WINDOWS
            _SILENCE_CXX17_CODECVT_HEADER_DEPRECATION_WARNING)
    target_compile_options(SimpleKafka1C PRIVATE /utf-8)
else ()
    target_link_options(SimpleKafka1C PRIVATE)
endif ()
