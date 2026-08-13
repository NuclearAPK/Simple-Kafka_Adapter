cmake_minimum_required(VERSION 3.20)
project(avro_selftest LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Статический CRT (/MT) — как в основном CMakeLists.txt; иначе линковка с
# библиотеками триплета x64-windows-static падает на LNK2038 RuntimeLibrary.
set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")

# Same workaround as the main CMakeLists: some avro-cpp ports ship a config that
# aliases avro-cpp::avrocpp before defining it. Pre-create it as a stub.
if(NOT TARGET avro-cpp::avrocpp)
    add_library(avro-cpp::avrocpp INTERFACE IMPORTED)
endif()
find_package(avro-cpp CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)

# avro_selftest — декод реальных .bin по схеме (нужны файлы, см. main.cpp)
add_executable(avro_selftest main.cpp)

foreach(tgt avro_selftest)
    # Mirror the link selection used by the main component (src target).
    if(TARGET avro-cpp::avrocpp_s)
        target_link_libraries(${tgt} PRIVATE avro-cpp::avrocpp_s)
    elseif(TARGET avro-cpp::avrocpp_static)
        target_link_libraries(${tgt} PRIVATE avro-cpp::avrocpp_static)
    elseif(TARGET avro-cpp::avrocpp)
        target_link_libraries(${tgt} PRIVATE avro-cpp::avrocpp)
    else()
        message(FATAL_ERROR "Could not find avro-cpp target")
    endif()

    target_link_libraries(${tgt} PRIVATE fmt::fmt)

    # Same fmt >= 11 workaround as the main component: avro/Exception.hh needs
    # fmt::format() which now lives in <fmt/format.h>.
    if (MSVC)
        target_compile_options(${tgt} PRIVATE /FI fmt/format.h /utf-8)
    else()
        target_compile_options(${tgt} PRIVATE -include fmt/format.h)
    endif()
endforeach()
