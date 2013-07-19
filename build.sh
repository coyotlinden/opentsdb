#!/bin/bash
set -xe
if [ "$1" = "debian" ]; then
    exec dpkg-buildpackage -A -uc -us -rfakeroot
else
    test -f configure || ./bootstrap
    test -d build || mkdir build
    cd build
    test -f Makefile || ../configure "$@"
    exec make "$@"
fi
