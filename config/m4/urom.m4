#
# Copyright (c) 2023, NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# See file LICENSE for terms.
#

AC_DEFUN([CHECK_UROM],[
AS_IF([test "x$urom_checked" != "xyes"],[
    urom_happy="no"

    AC_ARG_WITH([urom],
            [AS_HELP_STRING([--with-urom=(DIR)], [Enable the use of UROM (default is guess).])],
            [], [with_urom=guess])

    AS_IF([test "x$with_urom" != "xno"],
    [
        save_CPPFLAGS="$CPPFLAGS $UCS_CPPFLAGS"
        save_CFLAGS="$CFLAGS"
        save_LDFLAGS="$LDFLAGS"

        AS_IF([test ! -z "$with_urom" -a "x$with_urom" != "xyes" -a "x$with_urom" != "xguess"],
        [
            AS_IF([test ! -d $with_urom],
                  [AC_MSG_ERROR([Provided "--with-urom=${with_urom}" location does not exist])])
            check_urom_dir="$with_urom"
            check_urom_libdir="$with_urom/lib"
            CPPFLAGS="-I$with_urom/include $save_CPPFLAGS"
            LDFLAGS="-L$check_urom_libdir $save_LDFLAGS"
        ])

        AS_IF([test ! -z "$with_urom_libdir" -a "x$with_urom_libdir" != "xyes"],
        [
            check_urom_libdir="$with_urom_libdir"
            LDFLAGS="-L$check_urom_libdir $save_LDFLAGS"
        ])

        AC_CHECK_HEADERS([urom/api/urom.h],
        [
            AC_CHECK_LIB([urom], [urom_service_connect],
            [
                urom_happy="yes"
            ],
            [
                echo "CPPFLAGS: $CPPFLAGS"
                urom_happy="no"
            ], [-lurom])
        ],
        [
            urom_happy="no"
        ])

        AS_IF([test "x$urom_happy" = "xyes"],
        [
            AS_IF([test "x$check_urom_dir" != "x"],
            [
                AC_MSG_RESULT([UROM dir: $check_urom_dir])
                AC_SUBST(UROM_CPPFLAGS, "-I$check_urom_dir/include/ $urom_old_headers")
            ])

            AS_IF([test "x$check_urom_libdir" != "x"],
            [
                AC_SUBST(UROM_LDFLAGS, "-L$check_urom_libdir")
            ])

            AC_SUBST(UROM_LIBADD, "-lurom")
            AC_DEFINE([HAVE_UROM], 1, [Enable UROM support])
        ],
        [
            AS_IF([test "x$with_urom" != "xguess"],
            [
                AC_MSG_ERROR([UROM support is requested but UROM packages cannot be found! $CPPFLAGS $LDFLAGS])
            ],
            [
                AC_MSG_WARN([UROM not found])
            ])
        ])

        CFLAGS="$save_CFLAGS"
        CPPFLAGS="$save_CPPFLAGS"
        LDFLAGS="$save_LDFLAGS"
    ],
    [
        AC_MSG_WARN([UROM was explicitly disabled])
    ])
    urom_checked=yes
    AM_CONDITIONAL([HAVE_UROM], [test "x$urom_happy" != xno])
])])
