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

# Try to detect Arrow installation
# - macOS: Homebrew prefix
# - MSYS2 UCRT64: /ucrt64
# - Linux: /usr/local (common for source installs)
ifneq ($(findstring _NT-,$(UNAME_S)),)
	ARROW_HOME ?= /ucrt64
else
	ARROW_HOME ?= $(shell brew --prefix apache-arrow 2>/dev/null || echo /usr/local)
endif
ARROW_INCLUDE = -I$(ARROW_HOME)/include -I../../src
ARROW_LIBS = -L$(ARROW_HOME)/lib -larrow -lparquet

# Link against main FlintDB library
FlintDB_LIB = -L../../lib -lflintdb

# Windows (MSYS2) native build: uname contains *_NT-* (e.g., UCRT64_NT-10.0-22631)
ifneq ($(findstring _NT-,$(UNAME_S)),)
	TARGET = libflintdb_parquet.dll
else ifeq ($(UNAME_S),Darwin)
    TARGET = libflintdb_parquet.dylib
    LDFLAGS += -dynamiclib -install_name @rpath/libflintdb_parquet.dylib
	# rpath for dependencies: Arrow/Parquet and the core FlintDB next to this plugin
	LDFLAGS += -Wl,-rpath,$(ARROW_HOME)/lib -Wl,-rpath,@loader_path
else
    TARGET = libflintdb_parquet.so
	# Use $ORIGIN so the plugin can find libflintdb.so in the same directory
	LDFLAGS += -Wl,-rpath=$(ARROW_HOME)/lib -Wl,-rpath,'$$ORIGIN'
endif

CPP_SOURCES = parquet_plugin.cpp
C_SOURCES = parquet_plugin_adapter.c parquetfile.c
OBJECTS = $(CPP_SOURCES:.cpp=.o) $(C_SOURCES:.c=.o)
HEADERS = parquet_plugin.h

.PHONY: all clean install test-arrow

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $(LDFLAGS) -o $@ $^ $(ARROW_LIBS) $(FlintDB_LIB)
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
	@echo "ARROW_HOME: $(ARROW_HOME)"
	@test -d "$(ARROW_HOME)/include/arrow" || (echo "Error: Arrow headers not found in $(ARROW_HOME)/include/arrow"; exit 1)
	@test -f "$(ARROW_HOME)/lib/libarrow.dylib" -o -f "$(ARROW_HOME)/lib/libarrow.so" -o -f "$(ARROW_HOME)/lib/libarrow.dll.a" -o -f "$(ARROW_HOME)/bin/libarrow.dll" || (echo "Error: Arrow library not found in $(ARROW_HOME)/lib or $(ARROW_HOME)/bin"; exit 1)
	@echo "Arrow installation OK"
