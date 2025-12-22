# FlintDB JSONL Plugin Makefile

# This Makefile builds the FlintDB JSONL plugin as a shared library.
# It requires the cJSON library to be installed on the system.
# sudo apt-get install libcjson-dev
# sudo dnf install cjson-devel
# brew install cjson
# packman -S cjson

# Cross-compile support
# Usage: make CROSS_COMPILE=mingw
CROSS_COMPILE ?= none

ifeq ($(CROSS_COMPILE),mingw)
    CC = x86_64-w64-mingw32-gcc
    TARGET = libflintdb_jsonl.dll
    LDFLAGS_EXTRA = -static-libgcc
    # For MinGW, you need to build or install cJSON for Windows
    # This is complex, so we'll error out for now
    $(warning JSONL plugin for MinGW requires cJSON built for Windows. Set CJSON_HOME appropriately.)
else
    CC = gcc
endif
CFLAGS = -fPIC -O2 -Wall -DNDEBUG
LDFLAGS = -shared

# Platform detection (used for sensible default dependency prefixes)
UNAME_S := $(shell uname -s)

# Try to detect cJSON installation
# - macOS: Homebrew prefix
# - MSYS2 UCRT64: /ucrt64
# - Linux: /usr/local (common for source installs)
ifneq ($(findstring _NT-,$(UNAME_S)),)
	CJSON_HOME ?= /ucrt64
else
	CJSON_HOME ?= $(shell brew --prefix cjson 2>/dev/null || echo /usr/local)
endif
CJSON_INCLUDE = -I$(CJSON_HOME)/include -I../../src
CJSON_LIBS = -L$(CJSON_HOME)/lib -lcjson

# Link against main FlintDB library and zlib
FlintDB_LIB = -L../../lib -lflintdb
ZLIB_LIB = -lz

# Windows (MSYS2) native build: uname contains *_NT-* (e.g., UCRT64_NT-10.0-22631)
ifneq ($(findstring _NT-,$(UNAME_S)),)
	TARGET = libflintdb_jsonl.dll
else ifeq ($(CROSS_COMPILE),mingw)
    # MinGW cross-compilation
    TARGET = libflintdb_jsonl.dll
    LDFLAGS += -static-libgcc
else ifeq ($(UNAME_S),Darwin)
    TARGET = libflintdb_jsonl.dylib
    LDFLAGS += -dynamiclib -install_name @rpath/libflintdb_jsonl.dylib
	# rpath for dependencies: cJSON and the core FlintDB next to this plugin
	LDFLAGS += -Wl,-rpath,$(CJSON_HOME)/lib -Wl,-rpath,@loader_path
else
    TARGET = libflintdb_jsonl.so
	# Use $ORIGIN so the plugin can find libflintdb.so in the same directory
	LDFLAGS += -Wl,-rpath=$(CJSON_HOME)/lib -Wl,-rpath,'$$ORIGIN'
endif

C_SOURCES = jsonl_plugin_adapter.c jsonlfile.c
OBJECTS = $(C_SOURCES:.c=.o)

.PHONY: all clean install test-cjson

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(LDFLAGS) -o $@ $^ $(CJSON_LIBS) $(FlintDB_LIB) $(ZLIB_LIB)
	@echo "Built $@"

%.o: %.c
	$(CC) $(CFLAGS) $(CJSON_INCLUDE) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(TARGET)

install: $(TARGET)
	@mkdir -p ../../lib
	@cp $(TARGET) ../../lib/
	@echo "Plugin installed at ../../lib/$(TARGET)"
	@echo "FlintDB will load this automatically when accessing .jsonl files"

# Test if cJSON is available
test-cjson:
	@echo "Testing cJSON installation..."
	@echo "CJSON_HOME: $(CJSON_HOME)"
	@test -d "$(CJSON_HOME)/include/cjson" || (echo "Error: cJSON headers not found in $(CJSON_HOME)/include/cjson"; exit 1)
	@test -f "$(CJSON_HOME)/lib/libcjson.dylib" -o -f "$(CJSON_HOME)/lib/libcjson.so" -o -f "$(CJSON_HOME)/lib/libcjson.dll.a" -o -f "$(CJSON_HOME)/bin/libcjson.dll" || (echo "Error: cJSON library not found in $(CJSON_HOME)/lib or $(CJSON_HOME)/bin"; exit 1)
	@echo "cJSON installation OK"
