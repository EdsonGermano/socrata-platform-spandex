#!/bin/bash
set -e

REALPATH=$(python -c "import os; print(os.path.realpath('$0'))")
BASEDIR="$(dirname "${REALPATH}")/.."

cd "$BASEDIR"

if [ -n "$1" ]; then
  if [ "$1" != "spandex-http" ] && [ "$1" != "spandex-secondary" ]; then
    echo "Subproject must be one of spandex-http or spandex-secondary"
    exit 1
  fi

  SUBPROJECTS=( $1 )
else
  SUBPROJECTS=( spandex-http spandex-secondary )
fi

for subproject in "${SUBPROJECTS}"; do
  JARFILE="$(ls -rt $subproject/target/scala-*/$subproject-assembly-*.jar 2>/dev/null | tail -n 1)"
  SRC_PATHS=($(find .  -maxdepth 2 -name 'src' -o -name '*.sbt' -o -name '*.scala'))
  if [ -z "$JARFILE" ] || find "${SRC_PATHS[@]}" -newer "$JARFILE" | egrep -q -v '(/target/)|(/bin/)'; then
    nice -n 19 sbt assembly >&2
    JARFILE="$(ls -rt $subproject/target/scala-*/$subproject-assembly-*.jar 2>/dev/null | tail -n 1)"
    touch "$JARFILE"
  fi
done

python -c "import os; print(os.path.realpath('$JARFILE'))"
