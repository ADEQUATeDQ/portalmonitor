#!/usr/bin/env sh
mkdir -p ~/src/
cd ~/src/
mv /tmp/blas.tgz .
tar xzf blas.tgz
cd BLAS
gfortran -O3 -std=legacy -m64 -fno-second-underscore -fPIC -c *.f
ar r libfblas.a *.o
ranlib libfblas.a
rm -rf *.o
export BLAS=~/src/BLAS/libfblas.a
