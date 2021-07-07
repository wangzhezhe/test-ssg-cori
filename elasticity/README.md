install necessary packages

```
spack install mochi-ssg@main%gcc@9.3.0 cflags="-g" 
spack install mochi-thallium@0.9.1%gcc@9.3.0
spack install mochi-mona@main%gcc@9.3.0 build_type=Debug
spack install spdlog@1.4.1%gcc@9.3.0
spack install tclap@1.2.2%gcc@9.3.0
```

env 

```
module load spack
module load cmake/3.18.2
export CRAYPE_LINK_TYPE=dynamic
module swap PrgEnv-intel PrgEnv-gnu
module swap gcc/8.3.0 gcc/9.3.0

spack load -r mochi-ssg@main%gcc@9.3.0
spack load -r mochi-thallium@0.9.1%gcc@9.3.0
spack load -r mochi-mona@main%gcc@9.3.0
spack load spdlog@1.4.1%gcc@9.3.0
spack load tclap@1.2.2%gcc@9.3.0

export MPICH_GNI_NDREG_ENTRIES=1024
export HG_NA_LOG_LEVEL=debug
export ABT_THREAD_STACKSIZE=2097152

```

in the build dir:
```
cmake .. -DCMAKE_CXX_COMPILER=g++ -DCMAKE_C_COMPILER=gcc -DCMAKE_BUILD_TYPE=Debug
```

run (update build path in the scripts firstly) 

```
sbatch ~/cworkspace/src/test-ssg-cori/elasticity/cori_sim_elasticity_join.scripts
```

