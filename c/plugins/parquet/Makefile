# FlintDB Parquet Plugin Makefile

# This Makefile builds the FlintDB Parquet plugin as a shared library.
# It requires the Apache Arrow C++ library to be installed on the system.
# Ubuntu/Debian: sudo apt-get install libarrow-dev libparquet-dev
# Rocky/RHEL 9: Build from source (see INSTALL.md for instructions)
# macOS: brew install apache-arrow
# Arch: pacman -S apache-arrow
# MinGW cross-compile: Not supported (Arrow for Windows is complex to build)

# Cross-compile support
# Usage: make CROSS_COMPILE=mingw
CROSS_COMPILE ?= none

ifeq ($(CROSS_COMPILE),mingw)
    $(error Parquet plugin cross-compilation for Windows is not supported. Arrow C++ library requires native Windows build environment.)
endif

CXX = g++
CC = gcc
CXXFLAGS = -std=c++17 -fPIC -O2 -Wall
CFLAGS = -fPIC -O2 -Wall -DNDEBUG -Wno-format-truncation
LDFLAGS = -shared

# Platform detection (used for sensible default dependency prefixes)
UNAME_S := $(shell uname -s)

# Arrow installation detection
#
# CI gotcha: ARROW_HOME may be pre-set to a wrong prefix (e.g. /usr/local)
# even when Arrow is installed from distro packages under /usr.
# To make builds robust, compute an effective prefix (ARROW_PREFIX) that must
# actually contain include/arrow.

# Prefer pkg-config if present.
ARROW_PKG_PREFIX := $(shell command -v pkg-config >/dev/null 2>&1 && pkg-config --exists arrow >/dev/null 2>&1 && pkg-config --variable=prefix arrow 2>/dev/null || true)

# Validate user-provided ARROW_HOME first (from env or make ARROW_HOME=...)
ARROW_PREFIX := $(strip $(ARROW_HOME))
ifneq ($(ARROW_PREFIX),)
	ARROW_HOME_VALID := $(shell test -d "$(ARROW_PREFIX)/include/arrow" && echo yes || echo no)
	ifneq ($(ARROW_HOME_VALID),yes)
		ARROW_PREFIX :=
	endif
endif

# Auto-detect if ARROW_HOME was empty or invalid
ifeq ($(strip $(ARROW_PREFIX)),)
	ifneq ($(strip $(ARROW_PKG_PREFIX)),)
		ARROW_PREFIX := $(ARROW_PKG_PREFIX)
	else ifneq ($(findstring _NT-,$(UNAME_S)),)
		ARROW_PREFIX := /ucrt64
	else ifeq ($(UNAME_S),Darwin)
		ARROW_PREFIX := $(shell brew --prefix apache-arrow 2>/dev/null || echo /usr/local)
	else
		ARROW_PREFIX := $(shell test -d /usr/include/arrow && echo /usr || echo /usr/local)
	endif
endif

ARROW_INCLUDE = -I$(ARROW_PREFIX)/include -I../../src
ARROW_LIBS = -L$(ARROW_PREFIX)/lib -larrow -lparquet

# dlopen/dlsym live in libdl on Linux, and in dlfcn-win32's libdl on MSYS2/MinGW.
DL_LIBS =

# Link against main FlintDB library
FlintDB_LIB = -L../../lib -lflintdb

# Windows (MSYS2) native build: uname contains *_NT-* (e.g., UCRT64_NT-10.0-22631)
ifneq ($(findstring _NT-,$(UNAME_S)),)
	TARGET = libflintdb_parquet.dll
	DL_LIBS = -ldl
else ifeq ($(UNAME_S),Darwin)
    TARGET = libflintdb_parquet.dylib
    LDFLAGS += -dynamiclib -install_name @rpath/libflintdb_parquet.dylib
	# rpath for dependencies: Arrow/Parquet and the core FlintDB next to this plugin
	LDFLAGS += -Wl,-rpath,$(ARROW_PREFIX)/lib -Wl,-rpath,@loader_path
else
    TARGET = libflintdb_parquet.so
	DL_LIBS = -ldl
	# Use $ORIGIN so the plugin can find libflintdb.so in the same directory
	LDFLAGS += -Wl,-rpath=$(ARROW_PREFIX)/lib -Wl,-rpath,'$$ORIGIN'
endif

CPP_SOURCES = parquet_plugin.cpp
C_SOURCES = parquet_plugin_adapter.c parquetfile.c
OBJECTS = $(CPP_SOURCES:.cpp=.o) $(C_SOURCES:.c=.o)
HEADERS = parquet_plugin.h

.PHONY: all clean install test-arrow

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $(LDFLAGS) -o $@ $^ $(ARROW_LIBS) $(DL_LIBS) $(FlintDB_LIB)
	@echo "Built $@"

%.o: %.cpp $(HEADERS)
	$(CXX) $(CXXFLAGS) $(ARROW_INCLUDE) -c $< -o $@

%.o: %.c $(HEADERS)
	$(CC) $(CFLAGS) $(ARROW_INCLUDE) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(TARGET)

install: $(TARGET)
	@mkdir -p ../../lib
	@cp $(TARGET) ../../lib/
	@echo "Plugin installed at ../../lib/$(TARGET)"
	@echo "FlintDB will load this automatically when accessing .parquet files"

# Test if Arrow is available
test-arrow:
	@echo "Testing Arrow installation..."
	@echo "ARROW_PREFIX: $(ARROW_PREFIX)"
	@test -d "$(ARROW_PREFIX)/include/arrow" || (echo "Error: Arrow headers not found in $(ARROW_PREFIX)/include/arrow"; exit 1)
	@test -f "$(ARROW_PREFIX)/lib/libarrow.dylib" -o -f "$(ARROW_PREFIX)/lib/libarrow.so" -o -f "$(ARROW_PREFIX)/lib/libarrow.dll.a" -o -f "$(ARROW_PREFIX)/bin/libarrow.dll" || (echo "Error: Arrow library not found in $(ARROW_PREFIX)/lib or $(ARROW_PREFIX)/bin"; exit 1)
	@echo "Arrow installation OK"
