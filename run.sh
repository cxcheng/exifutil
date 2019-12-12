#!/bin/bash
./build.sh
if [ $? -ne 0 ]; then
  exit 1
fi
echo "Running exiftool..."
$GOPATH/bin/exiftool $*

