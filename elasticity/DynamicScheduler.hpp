/*
 * (C) 2020 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef __DYNAMICSCHEDULER_HPP
#define __DYNAMICSCHEDULER_HPP

#include <dlfcn.h>
#include <mona.h>
#include <spdlog/spdlog.h>

#include <fstream>
#include <map>
#include <thallium.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <tuple>

#include "ErrorCodes.hpp"
#include "Exception.hpp"
#include "RequestResult.hpp"

// this provider is truncated and used by the simulation program, it just
// provide the capability to update member when there is join or leave, it does
// not provide the capability for loading the pipeline etc.
// it only provide a get m_get_mona_addr RPC for collecting mona addrs

using namespace std::string_literals;
namespace tl = thallium;

struct ProcessState {
  bool active = false;
  uint64_t iteration = 0;
};

// store the status for associated process in the same group
// when this need to be initilized?
std::shared_ptr<ProcessState> processState;

class DynamicScheduler : public tl::provider<DynamicScheduler> {
  auto id() const { return get_provider_id(); }  // for convenience

 public:
  // SSG
  tl::mutex m_ssg_mtx;
  tl::condition_variable m_ssg_cv;
  ssg_group_id_t m_gid;
  tl::pool m_pool;

  // Mona
  tl::mutex m_mona_mtx;
  tl::condition_variable m_mona_cv;
  mona_instance_t m_mona;
  std::string m_mona_self_addr;
  std::map<ssg_member_id_t, na_addr_t> m_mona_addresses;
  std::vector<na_addr_t> m_updated_addresses;

  // get mona addr RPC
  tl::remote_procedure m_get_mona_addr;

  // Pending processes
  size_t m_num_active_processes = 0;
  tl::mutex m_processes_mtx;
  tl::condition_variable m_processes_cv;

  DynamicScheduler(const tl::engine& engine, ssg_group_id_t gid, bool must_join,
                   mona_instance_t mona, uint16_t provider_id,
                   const tl::pool& pool)
      : tl::provider<DynamicScheduler>(engine, provider_id),
        m_gid(gid),
        m_pool(pool),
        m_mona(mona),
        m_get_mona_addr(define("colza_get_mona_addr",
                               &DynamicScheduler::getMonaAddress, pool)) {
    int ret;
    // init the processStatus
    processState = std::make_shared<ProcessState>();
    // for the initial processes, we use the must_join
    if (must_join) {
      ret = ssg_group_join(engine.get_margo_instance(), m_gid,
                           &DynamicScheduler::membershipUpdate,
                           static_cast<void*>(this));
      if (ret != SSG_SUCCESS) {
        throw Exception(ErrorCode::SSG_ERROR,
                        "Could not join SSG group (ssg_group_join returned "s +
                            std::to_string(ret) + ")");
      }
    } else {
      // when the server addr is updated, call this to update the membership
      ssg_group_add_membership_update_callback(
          m_gid, &DynamicScheduler::membershipUpdate, static_cast<void*>(this));
    }
    {
      spdlog::trace("start to create mona process");
      std::lock_guard<tl::mutex> lock(m_mona_mtx);
      na_addr_t my_mona_addr;
      na_return_t ret = mona_addr_self(m_mona, &my_mona_addr);
      if (ret != NA_SUCCESS)
        throw Exception(ErrorCode::MONA_ERROR,
                        "Could not get address from MoNA");
      char buf[256];
      na_size_t buf_size = 256;
      ret = mona_addr_to_string(m_mona, buf, &buf_size, my_mona_addr);
      mona_addr_free(m_mona, my_mona_addr);
      if (ret != NA_SUCCESS) {
        throw Exception(ErrorCode::MONA_ERROR,
                        "Could not serialize MoNA address");
      }
      m_mona_self_addr = buf;
      spdlog::trace("MoNA address: {}", m_mona_self_addr);
    }
    m_mona_cv.notify_all();
    // TODO, the new joined process hangs at there
    _resolveMonaAddresses();
    spdlog::trace("Registered process");
  }

  ~DynamicScheduler() {
    spdlog::trace("Deregistering process");
    ssg_group_remove_membership_update_callback(m_gid, &membershipUpdate,
                                                static_cast<void*>(this));
    {
      std::lock_guard<tl::mutex> lock(m_mona_mtx);
      for (auto& p : m_mona_addresses) {
        mona_addr_free(m_mona, p.second);
      }
      m_mona_addresses.clear();
    }
    spdlog::trace(" => done!");
  }

  // we do not need the configuration since we do not need to load the pipelines
  // in this provider
  RequestResult<int32_t> activate(uint64_t iteration) {
    spdlog::trace("[provider:{}] Received start request for sim start");
    RequestResult<int32_t> result;

    // if the state is active, could not join
    if (processState->active) {
      // if sim is in active status, we can not do the merge operation
      result.value() = (int)ErrorCode::PROCESS_IS_ACTIVE;
      result.success() = false;
      result.error() = "Sim is already active";
    } else if (processState->iteration != 0 &&
               processState->iteration >= iteration) {
      result.value() = (int)ErrorCode::INVALID_ITERATION;
      result.success() = false;
      result.error() = "Sim cannot be started at an inferior iteration number";
    } else {
      {
        std::lock_guard<tl::mutex> lock(m_processes_mtx);
        m_num_active_processes += 1;
      }
      spdlog::trace("sim successfuly active iteration {}", iteration);
      if (result.success()) {
        processState->iteration = iteration;
        processState->active = true;
      } else {
        {
          std::lock_guard<tl::mutex> lock(m_processes_mtx);
          m_num_active_processes -= 1;
        }
        m_processes_cv.notify_all();
      }
    }
    return result;
  }

  RequestResult<int32_t> deactivate(uint64_t iteration) {
    spdlog::trace("deactivate for iteration {}", iteration);
    RequestResult<int32_t> result;
    if (!processState->active) {
      result.value() = (int)ErrorCode::PIPELINE_NOT_ACTIVE;
      result.success() = false;
      spdlog::error("[provider:{}] process is not active", id());
      result.error() = "Pipeline is not active";
    } else if (processState->iteration != iteration) {
      result.value() = (int)ErrorCode::INVALID_ITERATION;
      result.success() = false;
      result.error() = "Invalid iteration";
      spdlog::error("[provider:{}] Invalid iteration ({})", id(), iteration);
    } else {
      if (result.success()) {
        {
          std::lock_guard<tl::mutex> lock(m_processes_mtx);
          processState->active = false;
          m_num_active_processes -= 1;
        }
        m_processes_cv.notify_all();
      }
    }
    return result;
  }

  void leave() {
    spdlog::trace("Received request to leave");
    {
      std::unique_lock<tl::mutex> lock(m_processes_mtx);
      while (m_num_active_processes != 0) {
        m_processes_cv.wait(lock);
      }
      spdlog::trace("All the processes are inactive, provider can leave");
      ssg_group_leave(m_gid);
      spdlog::trace("Left SSG group, calling finalize");
      get_engine().finalize();
    }
  }

  void getMonaAddress(const tl::request& req) {
    spdlog::trace("[provider:{}] Received request for MoNA address", id());
    RequestResult<std::string> result;
    {
      std::unique_lock<tl::mutex> guard(m_processes_mtx);
      while (m_mona_self_addr.empty()) {
        m_processes_cv.wait(guard);
      }
      result.value() = m_mona_self_addr;
    }
    req.respond(result);
  }

  na_addr_t _requestMonaAddressFromSSGMember(ssg_member_id_t member_id) {
    hg_addr_t hg_addr = HG_ADDR_NULL;

    tl::provider_handle ph;
    try {
      bool ok = false;
      while (!ok) {
        // try to avoid ssg get false group member
        ssg_get_group_member_addr(m_gid, member_id, &hg_addr);
        char addr[1024];
        hg_size_t addr_size = 1024;
        margo_addr_to_string(get_engine().get_margo_instance(), addr,
                             &addr_size, hg_addr);
        spdlog::trace(
            "_requestMonaAddressFromSSGMember member_id {} hg_addr {} "
            "providerid "
            "{}",
            member_id, std::string(addr), id());

        // TODO maybe we need to detect if the addr is valid here
        ph = tl::provider_handle(get_engine(), hg_addr, 0, false);
        ok = true;
      }
    } catch (const std::exception& e) {
      spdlog::critical(
          "Could not create provider handle from address to member {}: {} "
          "retry",
          member_id, e.what());
      tl::thread::sleep(get_engine(), 100);
      // throw;
    }
    RequestResult<std::string> result;
    bool ok = false;
    while (!ok) {
      try {
        result = m_get_mona_addr.on(ph)();
        ok = true;
      } catch (...) {
        // TODO improve that to retry only in relevant
        // cases and sleep for increasing amounts of time
        spdlog::trace(
            "Failed to get MoNA address of member {}, "
            "retrying...",
            member_id);
        tl::thread::sleep(get_engine(), 100);
      }
    }
    na_addr_t addr = NA_ADDR_NULL;
    na_return_t ret = mona_addr_lookup(m_mona, result.value().c_str(), &addr);
    if (ret != NA_SUCCESS)
      throw Exception(
          ErrorCode::MONA_ERROR,
          "mona_addr_lookup failed with error code "s + std::to_string(ret));
    spdlog::trace("Successfully obtained MoNA address of member {}", member_id);
    return addr;
  }

  void _resolveMonaAddresses() {
    // TODO we could speed up this function if the getMonaAddress RPC
    // were to piggy-back the addresses it already knows
    spdlog::trace("[provider:{}] Resolving MoNA addressed of SSG group", id());
    ssg_member_id_t self_id = SSG_MEMBER_ID_INVALID;
    int ret = ssg_get_self_id(get_engine().get_margo_instance(), &self_id);
    if (ret != SSG_SUCCESS) {
      throw Exception(
          ErrorCode::SSG_ERROR,
          "ssg_get_self_id failed with error code "s + std::to_string(ret));
    }

    int self_rank = -1;
    ret = ssg_get_group_member_rank(m_gid, self_id, &self_rank);

    if (ret != SSG_SUCCESS) {
      throw Exception(ErrorCode::SSG_ERROR,
                      "ssg_get_group_member_rank failed with error code "s +
                          std::to_string(ret));
    }

    int group_size = 0;
    ret = ssg_get_group_size(m_gid, &group_size);
    if (ret != SSG_SUCCESS) {
      throw Exception(
          ErrorCode::SSG_ERROR,
          "ssg_get_group_size failed with error code "s + std::to_string(ret));
    }
    std::vector<ssg_member_id_t> member_ids(group_size);
    ret = ssg_get_group_member_ids_from_range(m_gid, 0, group_size - 1,
                                              member_ids.data());
    if (ret != SSG_SUCCESS) {
      throw Exception(
          ErrorCode::SSG_ERROR,
          "ssg_get_group_member_ids_from_range failed with error code "s +
              std::to_string(ret));
    }

    spdlog::trace(
        "[provider:{}] ssg self_id {} self_rank {} group_size {} found in SSG "
        "group",
        id(), self_id, self_rank, group_size);

    decltype(m_mona_addresses) tmp_addresses;

    for (int i = 0; i < group_size; i++) {
      int j = (self_rank + i) % group_size;
      ssg_member_id_t member_id = member_ids[j];
      if (m_mona_addresses.count(member_id) != 0) continue;
      if (member_id == self_id) {
        na_addr_t self_mona_addr;
        mona_addr_self(m_mona, &self_mona_addr);
        tmp_addresses[member_id] = self_mona_addr;
      } else {
        tmp_addresses[member_id] = _requestMonaAddressFromSSGMember(member_id);
      }
    }
    {
      std::lock_guard<tl::mutex> lock(m_mona_mtx);
      m_mona_addresses.insert(tmp_addresses.begin(), tmp_addresses.end());
    }
    m_mona_cv.notify_all();
    spdlog::trace("[provider:{}] Done resolving MoNA addressed of SSG group",
                  id());
  }

  void _membershipUpdate(ssg_member_id_t member_id,
                         ssg_member_update_type_t update_type) {
    spdlog::trace("[provider:{}] Member {} updated", id(), member_id);
    // TODO use the provider's pool instead of self ES
    tl::xstream::self().make_thread(
        [this, member_id, update_type]() {
          if (update_type == SSG_MEMBER_JOINED) {
            spdlog::trace("[provider:{}] Member {} joined", id(), member_id);
            na_addr_t na_addr = _requestMonaAddressFromSSGMember(member_id);
            {
              std::lock_guard<tl::mutex> lock(m_mona_mtx);
              m_mona_addresses[member_id] = na_addr;
            }
            m_mona_cv.notify_all();
          } else {
            spdlog::trace("[provider:{}] Member {} left", id(), member_id);
            {
              std::lock_guard<tl::mutex> lock(m_mona_mtx);
              m_mona_addresses.erase(member_id);
            }
          }
          {
            std::lock_guard<tl::mutex> lock(m_mona_mtx);
            m_updated_addresses.clear();
            m_updated_addresses.reserve(m_mona_addresses.size());
            for (const auto& p : m_mona_addresses)
              m_updated_addresses.push_back(p.second);
          }
          // origianlly, we range all pipelines and call updateMonaAddresses
          // function we do not need it if there is no pipeline
          // the sim can access m_updated_addresses direaclty to acquire the
          // latest mona_addrs list
        },
        tl::anonymous());
  }

  static void membershipUpdate(void* p, ssg_member_id_t member_id,
                               ssg_member_update_type_t update_type) {
    auto provider = static_cast<DynamicScheduler*>(p);
    provider->_membershipUpdate(member_id, update_type);
  }
};  // namespace colza

#endif
