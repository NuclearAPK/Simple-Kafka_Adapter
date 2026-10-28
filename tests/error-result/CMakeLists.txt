# Error result test: loads SimpleKafka1C.dll like the 1C platform and checks the value a
# function returns when an exception reaches Component::CallAsFunc (specs/error-result-values.md).
# No broker and no dependencies beyond the Native API headers in include/, e.g.:
#   cmake -S tests/error-result -B out/build/error-result -G Ninja -DCMAKE_BUILD_TYPE=Release
#   cmake --build out/build/error-result
#   out/build/error-result/error_result out/build/x64-Release/SimpleKafka1C.dll
cmake_minimum_required(VERSION 3.20)
project(error_result CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
endif()

# The platform-side host is shared with the native harness
set(HOST_DIR ${CMAKE_CURRENT_SOURCE_DIR}/../native-harness)

add_executable(error_result main.cpp ${HOST_DIR}/addin_host.cpp)
target_include_directories(error_result PRIVATE ${HOST_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/../../include)

if(MSVC)
    target_compile_options(error_result PRIVATE /utf-8 /W4 /wd4201) # C4201: nameless unions in the 1C header types.h
    target_compile_definitions(error_result PRIVATE _CRT_SECURE_NO_WARNINGS NOMINMAX WIN32_LEAN_AND_MEAN)
else()
    target_link_libraries(error_result PRIVATE ${CMAKE_DL_LIBS})
endif()
