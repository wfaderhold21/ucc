#
# Copyright (c) 2025, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
#

# OpenCL UCC Plugins
# Memory Component (MC) and Execution Component (EC)
# Real, functional plugins that work with OpenCL devices

# Additional documentation
EXTRA_DIST = README.md Makefile MIGRATION_FROM_XPU.md FEATURES.md EXECUTOR_NOTES.md \
             PLUGIN_SUMMARY.md IMPLEMENTATION_COMPLETE.md

# Plugin sources
noinst_LTLIBRARIES =
noinst_PROGRAMS =

# Optional: Build plugins as part of UCC if explicitly enabled
# Disabled by default - users should build standalone
if BUILD_PLUGIN_OPENCL
plugindir = $(libdir)/ucc/contrib
plugin_LTLIBRARIES = libucc_mc_opencl.la libucc_ec_opencl.la

# Memory Component plugin
libucc_mc_opencl_la_SOURCES = mc_opencl.c
libucc_mc_opencl_la_CPPFLAGS = $(BASE_CPPFLAGS) \
                               -I$(top_srcdir)/src \
                               -I$(top_srcdir)/src/components/mc/base
libucc_mc_opencl_la_LDFLAGS = -module -avoid-version -shared
libucc_mc_opencl_la_LIBADD = -lOpenCL

# Execution Component plugin
libucc_ec_opencl_la_SOURCES = ec_opencl.c
libucc_ec_opencl_la_CPPFLAGS = $(BASE_CPPFLAGS) \
                               -I$(top_srcdir)/src \
                               -I$(top_srcdir)/src/components/ec/base
libucc_ec_opencl_la_LDFLAGS = -module -avoid-version -shared
libucc_ec_opencl_la_LIBADD = -lOpenCL
endif

# Always include source files in distribution
EXTRA_DIST += mc_opencl.c ec_opencl.c
