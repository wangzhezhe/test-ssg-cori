#include <ssg.h>
#include <ssg-mpi.h>
extern "C" {
#include <rdmacred.h>
}
#include <thallium.hpp>
#include <mpi.h>
#include <iostream>

namespace tl = thallium;

static int        rank, size;
static uint32_t   drc_cookie;
static tl::engine engine;

static void setup_credentials();
static void setup_ssg_group();

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int ret;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    ret = ssg_init();
    if(ret != SSG_SUCCESS) {
        std::cerr << "Could not initialize SSG" << std::endl;
        exit(-1);
    }

    setup_credentials();

    hg_init_info hii;
    memset(&hii, 0, sizeof(hii));
    std::string drc_cookie_str = std::to_string(drc_cookie);
//    std::cerr << "DRC cookie = " << drc_cookie_str << std::endl;
    hii.na_init_info.auth_key = drc_cookie_str.c_str();

//    std::cerr << "Creating engine" << std::endl;
    engine = tl::engine("ofi+gni", THALLIUM_SERVER_MODE, true, 2, &hii);

//    std::cerr << "Setting up SSG" << std::endl;
    setup_ssg_group();

//    std::cerr << "Server running" << std::endl;
    engine.wait_for_finalize();

    MPI_Finalize();
}

void setup_credentials() {
    uint32_t          drc_credential_id;
    drc_info_handle_t drc_credential_info;
    int               ret;

    if(rank == 0) {
        ret = drc_acquire(&drc_credential_id, DRC_FLAGS_FLEX_CREDENTIAL);
        if(ret != DRC_SUCCESS) {
            std::cerr << "drc_acquire failed (ret = " << ret << ")" << std::endl;
            exit(-1);
        }
    }

    MPI_Bcast(&drc_credential_id, 1, MPI_UINT32_T, 0, MPI_COMM_WORLD);

    ret = drc_access(drc_credential_id, 0, &drc_credential_info);
    if(ret != DRC_SUCCESS) {
        std::cerr << "drc_access failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }

    drc_cookie = drc_get_first_cookie(drc_credential_info);

    if(rank == 0) {
        ret = drc_grant(drc_credential_id, drc_get_wlm_id(), DRC_FLAGS_TARGET_WLM);
        if(ret != DRC_SUCCESS) {
            std::cerr << "drc_grant failed (ret = " << ret << ")" << std::endl;
            exit(-1);
        }
        std::cout << "Credential: " << drc_credential_id << std::endl;
    }
}

void setup_ssg_group() {
    ssg_group_config_t group_config           = SSG_GROUP_CONFIG_INITIALIZER;
    group_config.swim_period_length_ms        = 1000;
    group_config.swim_suspect_timeout_periods = 3;
    group_config.swim_subgroup_member_count   = 1;
    ssg_group_id_t gid = ssg_group_create_mpi(
            engine.get_margo_instance(),
            "mygroup", MPI_COMM_WORLD,
            &group_config, nullptr, nullptr);
    engine.push_prefinalize_callback([]() { ssg_finalize(); });
    if(rank == 0) {
        int ret = ssg_group_id_store("my_ssg_group.ssg", gid, SSG_ALL_MEMBERS);
        if(ret != SSG_SUCCESS) {
            std::cerr << "ssg_group_id_store failed (ret = " << ret << ")" << std::endl;
            exit(-1);
        }
    }
}
