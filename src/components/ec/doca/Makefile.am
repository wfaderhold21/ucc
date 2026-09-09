#
# Copyright (c) 2026, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#

sources =    \
	ec_doca.h \
	ec_doca.c \
	ec_doca_channel.h \
	ec_doca_channel.c \
	ec_doca_command.h

module_LTLIBRARIES        = libucc_ec_doca.la
libucc_ec_doca_la_SOURCES  = $(sources)
libucc_ec_doca_la_CPPFLAGS = $(AM_CPPFLAGS) $(BASE_CPPFLAGS)
libucc_ec_doca_la_CFLAGS   = $(BASE_CFLAGS)
libucc_ec_doca_la_LDFLAGS  = -version-info $(SOVERSION) --as-needed
libucc_ec_doca_la_LIBADD   = $(UCC_TOP_BUILDDIR)/src/libucc.la

include $(top_srcdir)/config/module.am
