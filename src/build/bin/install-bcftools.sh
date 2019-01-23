#!/bin/sh
set -ex
wget https://github.com/samtools/bcftools/releases/download/1.3/bcftools-1.3.tar.bz2
tar -xjvf bcftools-1.3.tar.bz2
cd bcftools-1.3 && ./configure --prefix=/usr && make && sudo make install
