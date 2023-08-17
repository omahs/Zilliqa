/*
 * Copyright (C) 2023 Zilliqa
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

#ifndef ZILLIQA_SRC_LIBPERSISTENCE_UPLOADER_H_
#define ZILLIQA_SRC_LIBPERSISTENCE_UPLOADER_H_

#include "common/Common.h"

#include "google/cloud/storage/client.h"

#include <boost/thread/executors/basic_thread_pool.hpp>
#include <boost/thread/future.hpp>

#include <filesystem>
#include <optional>

namespace gcs = ::google::cloud::storage;

namespace zil {
namespace persistence {

class Uploader {
 public:
  ~Uploader() noexcept;

  template <typename WebhookURLT, typename StoragePathT, typename BucketNameT,
            typename TestnetNameT>
  Uploader(WebhookURLT&& webhookUrl, std::chrono::seconds avgTxBlkTime,
           std::chrono::seconds avgDsBlkTime, bool backup,
           StoragePathT&& storagePath, BucketNameT&& bucketName,
           TestnetNameT&& testnetName, unsigned int threadCount)
      : m_webhookUrl{std::forward<WebhookURLT>(webhookUrl)},
        // m_avgTxBlkTime{avgTxBlkTime},
        // m_avgDsBlkTime{avgDsBlkTime},
        // m_backup{backup},
        m_storagePath{std::forward<StoragePathT>(storagePath)},
        m_bucketName{std::forward<BucketNameT>(bucketName)},
        m_testnetName{std::forward<TestnetNameT>(testnetName)},
        m_threadPool{threadCount} {}

  void Start();

 private:
  const std::string m_webhookUrl;
  // const bool m_backup;
  // const std::chrono::seconds m_avgTxBlkTime;
  // const std::chrono::seconds m_avgDsBlkTime;

  const std::filesystem::path m_storagePath;
  const std::string m_bucketName;
  const std::string m_testnetName;

  mutable gcs::Client m_client;
  boost::executors::basic_thread_pool m_threadPool;

#if 0
  std::string StatidDbURLPrefix() const {
    return "blockchain-data/" + m_testnetName + '/';
  }

  std::string PersistenceURLPrefix() const {
    return "incremental/" + m_testnetName + '/';
  }

  std::string StateDeltaURLPrefix() const {
    return "statedelta/" + m_testnetName + '/';
  }

  auto StoragePath() const { return m_storagePath; }
  auto StaticDbPath() const { return m_storagePath / "/historical-data"; }
  auto PersistencePath() const { return m_storagePath / "persistence"; }
  auto PersistenceDiffPath() const { return m_storagePath / "persistenceDiff"; }
  auto StateDeltaPath() const { return m_storagePath / "StateDeltaFromS3"; }

  bool IsUploadOngoing() const;
  std::optional<uint64_t> GetCurrentTxBlkNum() const;
  void UploadStaticDb();
  void UploadPersistenceAndStateDeltas();
  void UploadDiffs(uint64_t fromTxBlk, uint64_t toTxBlk,
                     const std::string& prefix,
                     const std::string& fileNamePrefix,
                     const std::filesystem::path& uploadPath,
                     bool excludePersistenceDiff = true);
  void UploadPersistenceDiff(uint64_t fromTxBlk, uint64_t toTxBlk);
  void UploadStateDeltaDiff(uint64_t fromTxBlk, uint64_t toTxBlk);
  std::vector<gcs::ListObjectsReader::value_type> RetrieveBucketObjects(
      const std::string& prefix, bool excludePersistenceDiff = true);

  using UploadFutures = std::vector<boost::future<
      std::pair<std::string, std::optional<std::filesystem::path> > > >;

  UploadFutures UploadBucketObjects(
      const std::vector<gcs::ListObjectsReader::value_type>& bucketObjects,
      const std::filesystem::path& outputPath);
#endif
};

}  // namespace persistence
}  // namespace zil

#endif  // ZILLIQA_SRC_LIBPERSISTENCE_UPLOADER_H_
