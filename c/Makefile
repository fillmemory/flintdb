# Makefile for FlintDB C library and CLI
# Windows support via MSYS2 UCRT64 shell
#
# Usage: 
# make                 # build CLI only
# make shared          # build shared library only
# make static         # build static library only
# make clean          # clean build artifacts
# 

# Enable parallel builds by default
MAKEFLAGS += -j$(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

ifeq ($(OS),Windows_NT)
	# Detect MSYS2 shell (MINGW64, UCRT64, MSYS, etc.)
	UNAME_S := $(shell uname -s 2>/dev/null)
	# Check for MSYS2 environment (MINGW64_NT, UCRT64_NT, MSYS_NT, etc.)
	ifneq ($(findstring _NT-,$(UNAME_S)),)
		# Check if this is UCRT64 or MinGW64
		MSYSTEM := $(shell echo $$MSYSTEM)
		ifeq ($(MSYSTEM),MINGW64)
$(error ERROR: MinGW64 is NOT supported. Please use MSYS2 UCRT64 shell instead. UCRT64 provides better C standard support and POSIX compatibility. Install: pacman -S mingw-w64-ucrt-x86_64-gcc mingw-w64-ucrt-x86_64-cjson)
		endif
		# Try to use ccache if available for faster rebuilds
		CCACHE := $(shell command -v ccache 2>/dev/null)
		ifneq ($(CCACHE),)
			CC = ccache gcc
		else
			CC = gcc
		endif
	AR = ar
	STD = c11  # MSYS2 may have issues with c23
	# MSYS2: no LTO, no march=native (can cause issues)
	# Disable -Wuse-after-free for GCC 13+ false positives
	# Add -s to strip symbols for smaller binaries
	# Force UTF-8 for source input and runtime multibyte encoding (MinGW GCC)
	CFLAGS_BASE = -Wall -Wextra -I./c -O3 -s -std=$(STD) -finput-charset=UTF-8 -fexec-charset=UTF-8 -Wformat=0 -Wno-unused-parameter -Wno-strict-aliasing -Wno-use-after-free -D_GNU_SOURCE
	LDFLAGS_BASE = -lc -lm -lpthread -lz -s
	else
$(error ERROR: Only MSYS2 UCRT64 is supported on Windows. Native Windows build (cmd.exe, PowerShell) is NOT supported. Please install MSYS2 from https://www.msys2.org/ and use UCRT64 shell)
	endif
else
	CC = gcc
	AR = ar
	STD = c2x  # Use c2x for native compilation (Linux/macOS)
	# Native compilation flags
	CFLAGS_BASE = -Wall -Wextra -I./c -I/usr/local/include -O3 -march=native -flto=auto -std=$(STD) -Wformat=0 -Wno-unused-parameter -Wno-strict-aliasing -D_GNU_SOURCE
	LDFLAGS_BASE = -lc -lm -lpthread -lz -L/usr/local/lib -flto
	
	# Check for liburing on Linux (optional for async io_uring support)
	UNAME_S := $(shell uname -s)
	ifeq ($(UNAME_S),Linux)
		LIBURING_EXISTS := $(shell pkg-config --exists liburing 2>/dev/null && echo yes || echo no)
		ifeq ($(LIBURING_EXISTS),yes)
			CFLAGS_BASE += $(shell pkg-config --cflags liburing) -DHAVE_LIBURING
			LDFLAGS_BASE += $(shell pkg-config --libs liburing)
		else
			$(warning liburing not found - WAL will use standard synchronous I/O)
			$(warning For async I/O performance, install: sudo apt-get install liburing-dev  or  sudo dnf install liburing-devel)
		endif
	endif
endif

# Optional: enable/disable NDEBUG (disabled by default for debug info)
# Usage: make NDEBUG=1  # enable NDEBUG to disable assertions
NDEBUG ?= 1
ifneq ($(filter 1 yes true on,$(NDEBUG)),)
	CFLAGS_BASE += -DNDEBUG
endif

CFLAGS = $(CFLAGS_BASE)
CFLAGS_SO  = -fPIC -DFLINTDB_SHARED -DFLINTDB_BUILD
#LDFLAGS = -lc -lm -lpthread -lz -llz4 -lzstd -lsnappy -L/usr/local/lib -flto
LDFLAGS = $(LDFLAGS_BASE)

LIB_CURRENT_VERSION ?= 0.0.1
LIB_COMPAT_VERSION  ?= 0.0.1
LIB_BASENAME ?= libflintdb.dylib
LIB_INSTALL_NAME ?= @rpath/$(LIB_BASENAME)


# Optional allocator selection (none|jemalloc|tcmalloc)
# Usage: make ALLOCATOR=jemalloc
ALLOCATOR ?= none


# Optional: enable small-string pooling in variant.c
# Usage examples:
#   make VARIANT_STRPOOL=1                 # enable with defaults
#   make VARIANT_STRPOOL=1 STRPOOL_SIZE=128    # adjust fixed block size
#   make VARIANT_STRPOOL=1 STRPOOL_CAPACITY=4096
VARIANT_STRPOOL ?= 1
STRPOOL_SIZE ?= 1024
STRPOOL_CAPACITY ?= 1024

ifneq ($(filter 1 yes true on,$(VARIANT_STRPOOL)),)
	CFLAGS += -DVARIANT_USE_STRPOOL -DVARIANT_STRPOOL_STR_SIZE=$(STRPOOL_SIZE)u -DVARIANT_STRPOOL_CAPACITY=$(STRPOOL_CAPACITY)u
endif

# Optional: control whether to build shared library (so/dylib)
# Usage examples:
#   make                  # build CLI only
#   make BUILD_SO=1       # build CLI + shared library
#   make shared           # build only the shared library
BUILD_SO ?= 0

# Optional: enable embedded HTML for WebUI
# Usage examples:
#   make EMBED_HTML=1     # build with embedded HTML
#   make                  # build without embedded HTML (requires webui/webui.html at runtime)
EMBED_HTML ?= 1

ifneq ($(filter 1 yes true on,$(EMBED_HTML)),)
	CFLAGS += -DEMBED_HTML
endif

# check /opt/homebrew/lib exists for macOS with Homebrew
ifneq ("$(wildcard /opt/homebrew/lib)","")
    CFLAGS += -I/opt/homebrew/include
    LDFLAGS += -L/opt/homebrew/lib
endif

SRC_DIR = ./src
OBJ_CLI_DIR = ./build/obj_cli
OBJ_SO_DIR  = ./build/obj_so
BIN_DIR = ./bin
LIB_DIR = ./lib
WEBUI_HTML = ./webui/webui.html
WEBUI_EMBEDDED_H = $(SRC_DIR)/webui_embedded.h

SOURCES = $(wildcard $(SRC_DIR)/*.c)
OBJECTS_CLI = $(patsubst $(SRC_DIR)/%.c, $(OBJ_CLI_DIR)/%.o, $(SOURCES))
OBJECTS_SO  = $(patsubst $(SRC_DIR)/%.c, $(OBJ_SO_DIR)/%.o, $(SOURCES))
TARGET_CLI = $(BIN_DIR)/flintdb
TARGET_SO  = $(LIB_DIR)/libflintdb.so
TARGET_A   = $(LIB_DIR)/libflintdb.a

BUILD_TIME := $(shell date +%Y%m%d%H%M%S)

# Detect the platform
ifeq ($(OS),Windows_NT)
    # Check if we're in MSYS2 environment (POSIX layer on Windows)
    UNAME_S := $(shell uname -s 2>/dev/null)
    ifneq ($(findstring _NT-,$(UNAME_S)),)
        # MSYS2 environment - use POSIX behavior
        PLATFORM = msys
        TARGET_SO = $(LIB_DIR)/flintdb.dll
        TARGET_CLI = $(BIN_DIR)/flintdb.exe
    else
        $(error ERROR: Only MSYS2 UCRT64 is supported on Windows)
    endif
else
    UNAME_S := $(shell uname -s)
    ifeq ($(UNAME_S),Linux)
        PLATFORM = linux
    endif
    ifeq ($(UNAME_S),Darwin)
        PLATFORM = macos
		TARGET_SO = $(LIB_DIR)/libflintdb.dylib
		TARGET_A  = $(LIB_DIR)/libflintdb.a
    endif
endif

# Platform-specific flags
ifeq ($(PLATFORM),msys)
	# MSYS2 environment - uses POSIX layer, no ws2_32 needed
	CFLAGS  += -DPATH_MAX=4096 -D_WIN32 -D_POSIX_=1 -DPATH_SEPARATOR='\\\\'
endif
ifeq ($(PLATFORM),linux)
	CFLAGS  += -Dlinux
    # Linux-specific flags (if any)
	RPATH_FLAG := -Wl,-rpath,\$$ORIGIN/../lib
endif
ifeq ($(PLATFORM),macos)
    # macOS-specific flags (if any)
	LINK_VERSION_FLAGS := -Wl,-install_name,$(LIB_INSTALL_NAME) -Wl,-current_version,$(LIB_CURRENT_VERSION) -Wl,-compatibility_version,$(LIB_COMPAT_VERSION)
	RPATH_FLAG := -Wl,-rpath,@executable_path/../lib
endif

# Link against an alternative allocator when requested
ifeq ($(ALLOCATOR),jemalloc)
	# Prefer Homebrew paths; lib search paths already appended above if present
	LDFLAGS += -ljemalloc
	# On macOS, you may also run with DYLD_INSERT_LIBRARIES to interpose without relink.
endif
ifeq ($(ALLOCATOR),tcmalloc)
	LDFLAGS += -ltcmalloc
endif

# Decide which targets to build by default
TARGETS := $(TARGET_CLI)
# Allow common truthy values for BUILD_SO: 1/yes/true/on
ifneq ($(filter 1 yes true on,$(BUILD_SO)),)
TARGETS += $(TARGET_SO)
endif

# all: build default targets
# When building plugins, we need the shared library first
ifeq ($(EMBED_HTML),1)
all: $(WEBUI_EMBEDDED_H) $(TARGET_SO) $(TARGET_A) $(TARGETS)
else
all: $(TARGET_SO) $(TARGET_A) $(TARGETS)
endif

# Generate embedded HTML header from webui.html (only if EMBED_HTML=1)
ifeq ($(EMBED_HTML),1)
$(WEBUI_EMBEDDED_H): $(WEBUI_HTML) webui/embed_html.sh
	@echo "Generating embedded HTML header..."
	@./webui/embed_html.sh $(WEBUI_HTML) $(WEBUI_EMBEDDED_H)
endif

$(TARGET_CLI): $(TARGET_SO)
	@mkdir -p $(BIN_DIR)
	$(CC) $(CFLAGS) -DVERSION='"$(LIB_CURRENT_VERSION)"' -DBUILD_TIME='"$(BUILD_TIME)"' -o $@ $(SRC_DIR)/cli.c -L$(LIB_DIR) -lflintdb $(RPATH_FLAG) $(LDFLAGS)

# Platform-specific shared library linker flag
SHARED_FLAG := -shared
ifeq ($(PLATFORM),macos)
SHARED_FLAG := -dynamiclib
endif

$(TARGET_SO): $(OBJECTS_SO)
	@mkdir -p $(LIB_DIR)
	$(CC) $(CFLAGS) $(CFLAGS_SO) $(SHARED_FLAG) -DBUILD_TIME='"$(BUILD_TIME)"'  -o $@ $^ $(LINK_VERSION_FLAGS) $(LDFLAGS)

$(TARGET_A): $(OBJECTS_SO)
	@mkdir -p $(LIB_DIR)
	@rm -f $@
	$(AR) rcs $@ $^
	@ranlib $@ 2>/dev/null || true

$(OBJ_CLI_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(OBJ_CLI_DIR)
	$(CC) $(CFLAGS) $(CFLAGS_CLI) -DVERSION='"$(LIB_CURRENT_VERSION)"' -DBUILD_TIME='"$(BUILD_TIME)"' -c -o $@ $<

$(OBJ_SO_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(OBJ_SO_DIR)
	$(CC) $(CFLAGS) $(CFLAGS_SO) -DBUILD_TIME='"$(BUILD_TIME)"' -c -o $@ $<



## Convenience targets
.PHONY: cli shared static ios
cli: $(TARGET_CLI)
shared: $(TARGET_SO)
static: $(TARGET_A)

# iOS build (XCFramework)
# Requires Xcode Command Line Tools.
ios:
	@bash ./build_ios.sh

clean:
	rm -rf $(OBJ_CLI_DIR) $(OBJ_SO_DIR) $(WEBUI_EMBEDDED_H)
	rm -rf ./build/ios

.PHONY: all clean
