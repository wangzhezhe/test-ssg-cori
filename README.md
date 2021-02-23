# Building

Setup spack and sds-repo, clone this repository and `cd` in it, then:

```
spack env create ssg-test spack.yaml
spack env activate ssg-test
spack install
spack env deactivate
```

Then to build the code:

```
spack env activate ssg-test
module swap PrgEnv-intel PrgEnv-gnu
module swap gcc/8.3.0 gcc/9.3.0
mkdir build
cd build
cmake ..
make
```

# Running

From the `build` directory:

```
export MPICH_GNI_NDREG_ENTRIES=1024
srun -C haswell -n 1 -c 1 --cpu_bind=cores ./test-server
# TODO
```
