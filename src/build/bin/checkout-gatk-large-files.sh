#!/bin/sh
set -ex
git lfs install
mkdir gatk
cd gatk
git init
git config core.sparseCheckout true
git remote add -f origin https://github.com/broadinstitute/gatk
echo "src/test/resources/large/*" > .git/info/sparse-checkout
git checkout master
du -sh .
