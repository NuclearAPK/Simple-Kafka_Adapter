# Reproducible RHEL 9 release build of SimpleKafka1C.so
#
# Rocky Linux 9 is ABI-compatible with RHEL 9 (same glibc 2.34, gcc 11.x),
# so the resulting library runs on RHEL 9 / CentOS Stream 9 / AlmaLinux 9 / Rocky 9.
#
# All third-party dependencies (librdkafka, OpenSSL, curl, protobuf, abseil,
# boost, avro-cpp, fmt, zlib, zstd, lz4, snappy, cyrus-sasl) are built from
# source by vcpkg as static libraries and linked into the .so.
# libstdc++/libgcc are linked statically as well, so the only runtime
# dependencies left are core glibc libraries.

ARG BASE_IMAGE=rockylinux/rockylinux:9

FROM ${BASE_IMAGE} AS toolchain

# Build toolchain and the few system packages vcpkg ports need while building.
RUN dnf -y install dnf-plugins-core epel-release \
    && dnf config-manager --set-enabled crb \
    && dnf -y install --allowerasing \
        gcc \
        gcc-c++ \
        libstdc++-static \
        make \
        cmake \
        ninja-build \
        git \
        curl \
        ca-certificates \
        zip \
        unzip \
        tar \
        xz \
        bzip2 \
        patch \
        file \
        which \
        diffutils \
        perl \
        perl-IPC-Cmd \
        perl-Data-Dumper \
        perl-FindBin \
        perl-File-Compare \
        perl-File-Copy \
        python3 \
        pkgconf-pkg-config \
        autoconf \
        autoconf-archive \
        automake \
        libtool \
        bison \
        flex \
        binutils \
    && dnf clean all

# ---------------------------------------------------------------------------
# vcpkg + dependencies
# ---------------------------------------------------------------------------
FROM toolchain AS deps

# Pin to a specific vcpkg commit/tag for reproducible dependency versions.
ARG VCPKG_REF=master
ENV VCPKG_ROOT=/opt/vcpkg \
    VCPKG_DISABLE_METRICS=1

RUN git clone https://github.com/microsoft/vcpkg.git ${VCPKG_ROOT} \
    && git -C ${VCPKG_ROOT} checkout ${VCPKG_REF} \
    && git -C ${VCPKG_ROOT} rev-parse HEAD > /opt/vcpkg-commit.txt \
    && ${VCPKG_ROOT}/bootstrap-vcpkg.sh -disableMetrics

# x64-linux is vcpkg's static-library triplet on Linux.
ARG TRIPLET=x64-linux
ENV TRIPLET=${TRIPLET}

# Cache mounts keep vcpkg's binary cache and downloads between builds on the
# same host, so a rebuild does not recompile every dependency from scratch.
RUN --mount=type=cache,target=/root/.cache/vcpkg \
    --mount=type=cache,target=/opt/vcpkg/downloads \
    ${VCPKG_ROOT}/vcpkg install --triplet ${TRIPLET} --clean-after-build \
        "librdkafka[core,ssl,sasl,zlib,zstd,curl]" \
        "avro-cpp[core,snappy]" \
        "curl[core,ssl]" \
        protobuf \
        abseil \
        utf8-range \
        boost-json \
        boost-container \
        boost-property-tree \
        fmt \
        zlib \
        snappy \
        openssl \
        libuuid

# ---------------------------------------------------------------------------
# Component build
# ---------------------------------------------------------------------------
FROM deps AS build

WORKDIR /src
COPY CMakeLists.txt /src/CMakeLists.txt
COPY include /src/include
COPY src /src/src

# Build-time only fixes for the copied sources; the repository is left untouched.
COPY docker/rhel9/patches /patches
RUN for p in /patches/*.patch; do patch -p1 -d /src -i "$p"; done

ARG BUILD_TYPE=Release

# Two extra compile/link settings are needed on Linux:
#
# * include/com.h (1C Native API) pulls in <uuid/uuid.h> and uses uuid_compare()/
#   uuid_parse(), so libuuid is added explicitly - as a static library from
#   vcpkg, not as a runtime dependency on the system libuuid.so.1.
#
# * avro-cpp 1.12.1 headers call fmt::format() but only include <fmt/base.h>.
#   Since fmt 11 that declaration lives in <fmt/format.h>, so it is force
#   included ahead of every translation unit.
#
# libstdc++ is linked statically, libgcc is not: -static-libgcc pulls the
# unwinder into the library, and that unwinder references _dl_find_object@
# GLIBC_2.35, which is missing on early RHEL 9 minor releases. With the shared
# libgcc_s.so.1 (present on every RHEL 9 system) the highest required symbol
# version stays at GLIBC_2.34.
RUN cmake -S /src -B /build -G Ninja \
        -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
        -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake \
        -DVCPKG_TARGET_TRIPLET=${TRIPLET} \
        -DCMAKE_CXX_FLAGS="-isystem ${VCPKG_ROOT}/installed/${TRIPLET}/include -include fmt/format.h" \
        -DCMAKE_SHARED_LINKER_FLAGS="-static-libstdc++ -L${VCPKG_ROOT}/installed/${TRIPLET}/lib -luuid" \
    && cmake --build /build

# Strip and collect build provenance next to the artifact.
RUN mkdir -p /artifact \
    && cp /build/libSimpleKafka1C.so /artifact/SimpleKafka1C.so \
    && strip --strip-unneeded /artifact/SimpleKafka1C.so \
    && { \
        echo "base image      : $(cat /etc/redhat-release)"; \
        echo "gcc             : $(gcc -dumpversion)"; \
        echo "glibc (build)   : $(getconf GNU_LIBC_VERSION)"; \
        echo "vcpkg commit    : $(cat /opt/vcpkg-commit.txt)"; \
        echo "triplet         : ${TRIPLET}"; \
        echo "build type      : ${BUILD_TYPE}"; \
        echo "sha256          : $(sha256sum /artifact/SimpleKafka1C.so | cut -d' ' -f1)"; \
        echo ""; \
        echo "== ldd =="; ldd /artifact/SimpleKafka1C.so; \
        echo ""; \
        echo "== required glibc symbol versions =="; \
        objdump -T /artifact/SimpleKafka1C.so | grep -o 'GLIBC_[0-9.]*' | sort -uV; \
        echo "max required: $(objdump -T /artifact/SimpleKafka1C.so | grep -o 'GLIBC_[0-9.]*' | sort -uV | tail -1)"; \
        echo ""; \
        echo "== exported symbols =="; \
        nm -D --defined-only /artifact/SimpleKafka1C.so | awk '{print $NF}' | sort; \
    } > /artifact/build-info.txt 2>&1

# ---------------------------------------------------------------------------
# Export stage: `docker build --target export --output <dir>` writes the
# artifact to the host without creating an image.
# ---------------------------------------------------------------------------
FROM scratch AS export
COPY --from=build /artifact/ /
