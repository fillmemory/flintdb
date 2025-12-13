#!/bin/bash

# This script is only for Linux systems
if [[ "$(uname -s)" != "Linux" ]]; then
    echo "Error: This script is only for Linux systems."
    echo "Current OS: $(uname -s)"
    echo ""
    echo "For other platforms:"
    echo "  - macOS: brew install apache-arrow"
    echo "  - Windows: Use vcpkg or conda"
    exit 1
fi

# 1. 필요한 개발 도구 설치
sudo dnf install -y cmake gcc-c++ git wget

# 2. Arrow 소스 다운로드 및 빌드
cd /tmp
wget https://archive.apache.org/dist/arrow/arrow-14.0.1/apache-arrow-14.0.1.tar.gz
tar xzf apache-arrow-14.0.1.tar.gz
cd apache-arrow-14.0.1/cpp
mkdir build && cd build

# 3. CMake 설정 (Parquet 포함)
cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DARROW_PARQUET=ON \
  -DARROW_WITH_SNAPPY=ON \
  -DARROW_WITH_ZLIB=ON \
  -DCMAKE_INSTALL_PREFIX=/usr/local

# 4. 빌드 및 설치
make -j$(nproc)
sudo make install
sudo ldconfig