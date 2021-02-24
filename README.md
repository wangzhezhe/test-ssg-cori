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
export CRAYPE_LINK_TYPE=dynamic
module swap PrgEnv-intel PrgEnv-gnu
module swap gcc/8.3.0 gcc/9.3.0
spack env activate ssg-test
mkdir build
cd build
cmake ..
make
```

# Running

From the `build` directory:

```
export MPICH_GNI_NDREG_ENTRIES=1024
export HG_NA_LOG_LEVEL=debug
export ABT_THREAD_STACKSIZE=2097152
srun -C haswell -n 128 ./test-server
# one of the server will print "Credential: X", copy the X
```

In another terminal window, with current working directory set to `build`:
```
export MPICH_GNI_NDREG_ENTRIES=1024
export HG_NA_LOG_LEVEL=debug
export ABT_THREAD_STACKSIZE=2097152
srun -C haswell -n 1 ./test-client X # replace X with the copied value
```
