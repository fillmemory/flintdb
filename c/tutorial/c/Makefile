# Makefile for building the tutorial program against libflintdb.dylib
# Assumes libflintdb is built and located in ../lib
# and header files are in ../src
# Usage: 
#        make clean && make && ./tutorial 

CC ?= cc
STD ?= c23
CFLAGS ?= -O2 -Wall -Wextra -std=$(STD) \
	-I../../src -I/usr/local/include
LDFLAGS ?= -L../../lib -L/usr/local/lib \
	-lm -lpthread -lz

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Darwin)
	RPATH_FLAG := -Wl,-rpath,@loader_path/../../lib
endif
ifeq ($(UNAME_S),Linux)
	RPATH_FLAG := -Wl,-rpath,\$$ORIGIN/../../lib
endif

# Add Homebrew include/lib paths on Apple Silicon only if present
ifneq ("$(wildcard /opt/homebrew/lib)","")
	CFLAGS += -I/opt/homebrew/include
	LDFLAGS += -L/opt/homebrew/lib
endif

TARGET := tutorial
SRCS := tutorial.c
OBJS := $(SRCS:.c=.o)

.PHONY: all clean run

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -o $@ $^ -L../../lib $(RPATH_FLAG) -lflintdb $(LDFLAGS)

clean:
	rm -f $(OBJS) $(TARGET)

run: $(TARGET)
	$(TARGET)
