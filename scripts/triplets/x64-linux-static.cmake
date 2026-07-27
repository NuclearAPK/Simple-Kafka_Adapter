set(VCPKG_TARGET_ARCHITECTURE x64)
set(VCPKG_CRT_LINKAGE dynamic)
set(VCPKG_LIBRARY_LINKAGE static)
set(VCPKG_CMAKE_SYSTEM_NAME Linux)

# The 1C add-in is a SHARED object (.so) that statically links all vcpkg
# dependencies. Those static archives must be position-independent, otherwise
# the final shared library fails to link. Force -fPIC on every dependency.
set(VCPKG_C_FLAGS "-fPIC")
set(VCPKG_CXX_FLAGS "-fPIC")
