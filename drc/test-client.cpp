#include <ssg.h>
#include <ssg-mpi.h>
extern "C" {
#include <rdmacred.h>
}
#include <thallium.hpp>
#include <mpi.h>
#include <iostream>

namespace tl = thallium;

static uint32_t drc_cookie;
static tl::engine engine;

static void setup_credentials(const char* credential);
static void contact_ssg_group(margo_instance_id);

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    int ret;

    if(argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <credential>" << std::endl;
        exit(-1);
    }

    char* credential = argv[1];

    ret = ssg_init();
    if(ret != SSG_SUCCESS) {
        std::cerr << "Could not initialize SSG" << std::endl;
        exit(-1);
    }

    setup_credentials(credential);

    hg_init_info hii;
    memset(&hii, 0, sizeof(hii));
    std::string drc_cookie_str = std::to_string(drc_cookie);
    hii.na_init_info.auth_key = drc_cookie_str.c_str();

    engine = tl::engine("ofi+gni", THALLIUM_CLIENT_MODE, false, 0, &hii);
    engine.enable_remote_shutdown();

    contact_ssg_group(engine.get_margo_instance());

    engine.finalize();

    ssg_finalize();

    MPI_Finalize();
}

void setup_credentials(const char* credential) {
    uint32_t          drc_credential_id = atoi(credential);
    drc_info_handle_t drc_credential_info;
    int               ret;

    ret = drc_access(drc_credential_id, 0, &drc_credential_info);
    if(ret != DRC_SUCCESS) {
        std::cerr << "drc_access failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }

    drc_cookie = drc_get_first_cookie(drc_credential_info);
}

void contact_ssg_group(margo_instance_id mid) {
    ssg_group_id_t gid         = SSG_GROUP_ID_INVALID;
    int num_addrs              = SSG_ALL_MEMBERS;
    std::string ssg_group_file = "my_ssg_group.ssg";
    int ret                    = ssg_group_id_load(ssg_group_file.c_str(), &num_addrs, &gid);
    if(ret != SSG_SUCCESS) {
        std::cerr << "ssg_group_id_load failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    ret = ssg_group_observe(mid, gid);
    if(ret != SSG_SUCCESS) {
        std::cerr << "ssg_group_observe failed (ret = " << ret << ")" << std::endl;
        exit(-1);
    }
    int group_size = ssg_get_group_size(gid);
    std::cout << "Group size = " << group_size << std::endl;
    std::cout << "Addresses:" << std::endl;
    for(int i = 0; i < group_size; i++) {
        char* addr = ssg_group_id_get_addr_str(gid, i);
        if(addr)
            std::cout << "[" << i << "] " << addr << std::endl;
        else
            std::cout << "[" << i << "] null" << std::endl;
        free(addr);
    }
}
