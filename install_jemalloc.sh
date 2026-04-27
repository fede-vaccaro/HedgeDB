#!/bin/bash

git clone https://github.com/jemalloc/jemalloc.git

cd jemalloc

./autogen.sh

./configure

make -j$(nproc)

sudo make install