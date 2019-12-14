#!/bin/bash
./build.sh
if [ $? -ne 0 ]; then
  exit 1
fi
echo "Running exifutil..."
$GOPATH/bin/exifutil $*

