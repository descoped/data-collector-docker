#!/usr/bin/env bash

if [ "$1" = '/run.sh' -a "$(id -u)" = '0' ];
then
  chown -R $UID:$GID /conf
  chown -R $UID:$UID /certs

  exec gosu $UID "$BASH_SOURCE" "$@"
fi

umask 0027
exec "$@"
