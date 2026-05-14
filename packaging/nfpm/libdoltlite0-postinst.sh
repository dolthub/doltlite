#!/bin/sh
# postinst for libdoltlite0. Refresh /etc/ld.so.cache so the dynamic
# loader can find libdoltlite.so.0 immediately after `dpkg -i`. The
# interest-noawait ldconfig trigger in libdoltlite0.yaml is the
# Debian-convention way of signalling this, but it only fires when
# something activates the `ldconfig` trigger — which doesn't happen
# on a standalone `dpkg -i` of a single library. Running ldconfig
# here makes it unconditional.
set -e

if [ "$1" = "configure" ]; then
  ldconfig
fi
