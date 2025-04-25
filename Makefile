# SPDX-License-Identifier: BSD-3-Clause
# Copyright(c) 2010-2014 Intel Corporation

# binary names
APP_SENDER = math-sender
APP_RECEIVER = math-receiver

# all source files
SRCS_SENDER := sender.c
SRCS_RECEIVER := receiver.c

# Build using pkg-config variables if possible
$(shell pkg-config --exists libdpdk)
ifeq ($(.SHELLSTATUS),0)

all: sender receiver
.PHONY: sender receiver shared static clean
sender: build/$(APP_SENDER)-shared
	ln -sf $(APP_SENDER)-shared build/$(APP_SENDER)
receiver: build/$(APP_RECEIVER)-shared
	ln -sf $(APP_RECEIVER)-shared build/$(APP_RECEIVER)

PC_FILE := $(shell pkg-config --path libdpdk)
CFLAGS += -O3 $(shell pkg-config --cflags libdpdk)
LDFLAGS_SHARED = $(shell pkg-config --libs libdpdk)
LDFLAGS_STATIC = -Wl,-Bstatic $(shell pkg-config --static --libs libdpdk)

build/$(APP_SENDER)-shared: $(SRCS_SENDER) Makefile $(PC_FILE) | build
	$(CC) $(CFLAGS) $(SRCS_SENDER) -o $@ $(LDFLAGS) $(LDFLAGS_SHARED)
build/$(APP_RECEIVER)-shared: $(SRCS_RECEIVER) Makefile $(PC_FILE) | build
	$(CC) $(CFLAGS) $(SRCS_RECEIVER) -o $@ $(LDFLAGS) $(LDFLAGS_SHARED)
build:
	@mkdir -p $@

clean:
	rm -f build/$(APP_SENDER) build/$(APP_SENDER)-shared build/$(APP_RECEIVER) build/$(APP_RECEIVER)-shared
	rmdir --ignore-fail-on-non-empty build

else

ifeq ($(RTE_SDK),)
$(error "Please define RTE_SDK environment variable")
endif

# Default target, detect a build directory, by looking for a path with a .config
RTE_TARGET ?= $(notdir $(abspath $(dir $(firstword $(wildcard $(RTE_SDK)/*/.config)))))

include $(RTE_SDK)/mk/rte.vars.mk

CFLAGS += -O3
CFLAGS += $(WERROR_FLAGS)

include $(RTE_SDK)/mk/rte.extapp.mk

endif