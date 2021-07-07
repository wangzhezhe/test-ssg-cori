/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <mpi.h>
#include <spdlog/spdlog.h>
#include <ssg-mpi.h>
#include <tclap/CmdLine.h>

#include "DynamicScheduler.hpp"
#include <fstream>
#include <iostream>
#include <vector>

#ifdef USE_GNI
extern "C"
{
#include <rdmacred.h>
}
#endif

namespace tl = thallium;

static std::string g_address = "na+sm";
static int g_num_threads = 0;
static std::string g_log_level = "info";
static std::string g_ssg_file = "";
static std::string g_config_file = "";
static bool g_join = false;
static unsigned g_swim_period_ms = 1000;
static int64_t g_drc_credential = -1;
static uint64_t g_num_iterations = 10;

static void parse_command_line(int argc, char** argv, int rank);
static int64_t setup_credentials();
static uint32_t get_credential_cookie(int64_t credential_id);
static void update_group_file(
  void* group_data, ssg_member_id_t member_id, ssg_member_update_type_t update_type);

int main(int argc, char** argv)
{

  // Initialize MPI
  MPI_Init(&argc, &argv);
  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  parse_command_line(argc, argv, rank);
  spdlog::set_level(spdlog::level::from_str(g_log_level));

  // Initialize SSG
  int ret = ssg_init();
  if (ret != SSG_SUCCESS)
  {
    spdlog::critical("Could not initialize SSG");
    exit(-1);
  }
  ssg_group_id_t gid;

  uint32_t cookie = 0;
  if (!g_join)
  {
    g_drc_credential = g_drc_credential == -1 ? setup_credentials() : g_drc_credential;
    spdlog::trace("Credential id is {}", g_drc_credential);
    cookie = get_credential_cookie(g_drc_credential);
  }
  else
  {
    int num_addrs = SSG_ALL_MEMBERS;
    ret = ssg_group_id_load(g_ssg_file.c_str(), &num_addrs, &gid);
    if (ret != SSG_SUCCESS)
    {
      spdlog::critical("Could not load group id from file");
      exit(-1);
    }
    ssg_group_id_get_cred(gid, &g_drc_credential);
    spdlog::trace("Credential id read from SSG file: {}", g_drc_credential);
    if (g_drc_credential != -1)
      cookie = get_credential_cookie(g_drc_credential);
  }

  hg_init_info hii;
  memset(&hii, 0, sizeof(hii));
  std::string cookie_str = std::to_string(cookie);
  if (g_drc_credential != -1)
    hii.na_init_info.auth_key = cookie_str.c_str();

  // we use one RPC thread to run SSG RPCs
  tl::engine engine(g_address, THALLIUM_SERVER_MODE, true, 4, &hii);

  // if it is not g_join, it is the initial process
  if (!g_join)
  {
    // Create SSG group using MPI
    ssg_group_config_t group_config = SSG_GROUP_CONFIG_INITIALIZER;
    group_config.swim_period_length_ms = g_swim_period_ms;
    group_config.swim_suspect_timeout_periods = 3;
    group_config.swim_subgroup_member_count = 1;
    group_config.ssg_credential = g_drc_credential;
    ssg_group_create_mpi(engine.get_margo_instance(), "mygroup", MPI_COMM_WORLD, &group_config,
      nullptr, nullptr, &gid);
  }
  else
  {
    // ssg_group_join will be called in the provider constructor
  }
  engine.push_prefinalize_callback([]() {
    spdlog::trace("Finalizing SSG...");
    ssg_finalize();
    spdlog::trace("SSG finalized");
  });

  // Write SSG file
  if (rank == 0 && !g_ssg_file.empty() && !g_join)
  {
    int ret = ssg_group_id_store(g_ssg_file.c_str(), gid, SSG_ALL_MEMBERS);
    if (ret != SSG_SUCCESS)
    {
      spdlog::critical("Could not store SSG file {}", g_ssg_file);
      exit(-1);
    }
  }

  // Create Mona instance
  mona_instance_t mona = mona_init_thread(g_address.c_str(), NA_TRUE, &hii.na_init_info, NA_TRUE);

  // Print MoNA address for information
  na_addr_t mona_addr;
  mona_addr_self(mona, &mona_addr);
  std::vector<char> mona_addr_buf(256);
  na_size_t mona_addr_size = 256;
  mona_addr_to_string(mona, mona_addr_buf.data(), &mona_addr_size, mona_addr);
  spdlog::debug("MoNA address is {}", mona_addr_buf.data());
  mona_addr_free(mona, mona_addr);

  // create ESs
  std::vector<tl::managed<tl::xstream> > colza_xstreams;
  tl::managed<tl::pool> managed_colza_pool = tl::pool::create(tl::pool::access::mpmc);
  for (int i = 0; i < g_num_threads; i++)
  {
    colza_xstreams.push_back(
      tl::xstream::create(tl::scheduler::predef::basic_wait, *managed_colza_pool));
  }
  tl::pool colza_pool;
  if (g_num_threads == 0)
  {
    colza_pool = *managed_colza_pool;
  }
  else
  {
    colza_pool = tl::xstream::self().get_main_pools(1)[0];
  }
  engine.push_finalize_callback([&colza_xstreams, &colza_pool]() {
    spdlog::trace("Joining Colza xstreams");
    for (auto& es : colza_xstreams)
    {
      es->make_thread([]() { tl::xstream::self().exit(); }, tl::anonymous());
    }
    usleep(1);
    for (auto& es : colza_xstreams)
    {
      es->join();
    }
    colza_xstreams.clear();
    spdlog::trace("Colza xstreams joined");
  });

  // colza::Provider provider(engine, gid, g_join, mona, 0, config, colza_pool);
  // tl::engine* enginePtr, ssg_group_id_t gid, bool must_join,
  //                 mona_instance_t mona, const tl::pool& pool
  // it looks the program hangs there when the g_join is true
  DynamicScheduler dscheduler(engine, gid, g_join, mona, 0, colza_pool);

  // Add a callback to rewrite the SSG file when the group membership changes
  ssg_group_add_membership_update_callback(gid, update_group_file, reinterpret_cast<void*>(gid));

  spdlog::info("Server running at address {}", (std::string)engine.self());

  for (int step = 0; step < g_num_iterations; step++)
  {
    // TODO, how to avoid the new added process hangs there?
    RequestResult<int32_t> results = dscheduler.activate(step);
    spdlog::trace(
      "sim iteration {}: results success {} info {}", step, results.success(), results.error());
    // get the addr used for current iteration
    // how to make sure every process has the same view then move to the next iteration?
    // how to make sure new added process start from a specific iteration?
    spdlog::trace("sim iteration {}: number of addresses is now {}", step,
      dscheduler.m_updated_addresses.size());
    sleep(5);
    dscheduler.deactivate(step);
  }

  spdlog::trace("Engine finalized, now finalizing MoNA...");
  mona_finalize(mona);
  spdlog::trace("MoNA finalized");

  MPI_Finalize();

  return 0;
}

void parse_command_line(int argc, char** argv, int rank)
{
  try
  {
    TCLAP::CmdLine cmd("Spawns a Colza daemon", ' ', "0.1");
    TCLAP::ValueArg<std::string> addressArg(
      "a", "address", "Address or protocol (e.g. ofi+tcp)", true, "", "string");
    TCLAP::ValueArg<int> numThreads(
      "t", "num-threads", "Number of threads for RPC handlers", false, 0, "int");
    TCLAP::ValueArg<std::string> logLevel("v", "verbose",
      "Log level (trace, debug, info, warning, error, critical, off)", false, "info", "string");
    TCLAP::ValueArg<std::string> ssgFile("s", "ssg-file", "SSG file name", false, "", "string");

    TCLAP::SwitchArg joinGroup("j", "join", "Join an existing group rather than create it", false);
    TCLAP::ValueArg<unsigned> swimPeriod(
      "p", "swim-period-length", "Length of the SWIM period in milliseconds", false, 1000, "int");
    TCLAP::ValueArg<int64_t> drc(
      "d", "drc-credential-id", "DRC credential ID, if already setup", false, -1, "int");
    TCLAP::ValueArg<unsigned> numIterations(
      "i", "iterations", "Number of iterations", false, 10, "int");

    cmd.add(addressArg);
    cmd.add(numThreads);
    cmd.add(logLevel);
    cmd.add(ssgFile);
    cmd.add(joinGroup);
    cmd.add(swimPeriod);
    cmd.add(drc);
    cmd.add(numIterations);

    cmd.parse(argc, argv);
    g_address = addressArg.getValue();
    g_num_threads = numThreads.getValue();
    g_log_level = logLevel.getValue();
    g_ssg_file = ssgFile.getValue();
    g_join = joinGroup.getValue();
    g_swim_period_ms = swimPeriod.getValue();
    g_drc_credential = drc.getValue();
    g_num_iterations = numIterations.getValue();

    if (rank == 0)
    {
      std::cout << "------configuraitons------" << std::endl;
      std::cout << "g_address: " << g_address << std::endl;
      std::cout << "g_num_threads: " << g_num_threads << std::endl;
      std::cout << "g_log_level: " << g_log_level << std::endl;
      std::cout << "g_ssg_file: " << g_ssg_file << std::endl;
      std::cout << "g_join: " << g_join << std::endl;
      std::cout << "g_swim_period_ms: " << g_swim_period_ms << std::endl;
      std::cout << "g_drc_credential: " << g_drc_credential << std::endl;
      std::cout << "g_num_iterations: " << g_num_iterations << std::endl;
      std::cout << "--------------------------" << std::endl;
    }
  }
  catch (TCLAP::ArgException& e)
  {
    std::cerr << "error: " << e.error() << " for arg " << e.argId() << std::endl;
    exit(-1);
  }
}

void update_group_file(void* group_data, ssg_member_id_t, ssg_member_update_type_t)
{
  ssg_group_id_t gid = reinterpret_cast<ssg_group_id_t>(group_data);
  int r = -1;
  ssg_get_group_self_rank(gid, &r);
  if (r != 0)
    return;
  int ret = ssg_group_id_store(g_ssg_file.c_str(), gid, SSG_ALL_MEMBERS);
  if (ret != SSG_SUCCESS)
  {
    spdlog::error("Could not store updated SSG file {}", g_ssg_file);
  }
}

int64_t setup_credentials()
{
  uint32_t drc_credential_id = -1;
#ifdef USE_GNI
  if (g_address.find("gni") == std::string::npos)
    return -1;

  int rank;
  int ret;

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  if (rank == 0)
  {
    ret = drc_acquire(&drc_credential_id, DRC_FLAGS_FLEX_CREDENTIAL);
    if (ret != DRC_SUCCESS)
    {
      spdlog::critical("drc_acquire failed (ret = {})", ret);
      exit(-1);
    }
  }

  MPI_Bcast(&drc_credential_id, 1, MPI_UINT32_T, 0, MPI_COMM_WORLD);

  if (rank == 0)
  {
    ret = drc_grant(drc_credential_id, drc_get_wlm_id(), DRC_FLAGS_TARGET_WLM);
    if (ret != DRC_SUCCESS)
    {
      spdlog::critical("drc_grant failed (ret = {})", ret);
      exit(-1);
    }
    spdlog::info("DRC credential id: {}", drc_credential_id);
  }

#endif
  return drc_credential_id;
}

uint32_t get_credential_cookie(int64_t credential_id)
{
  uint32_t drc_cookie = 0;

  if (credential_id < 0)
    return drc_cookie;
  if (g_address.find("gni") == std::string::npos)
    return drc_cookie;

#ifdef USE_GNI

  drc_info_handle_t drc_credential_info;
  int ret;

  ret = drc_access(credential_id, 0, &drc_credential_info);
  if (ret != DRC_SUCCESS)
  {
    spdlog::critical("drc_access failed (ret = {})", ret);
    exit(-1);
  }

  drc_cookie = drc_get_first_cookie(drc_credential_info);

#endif
  return drc_cookie;
}
