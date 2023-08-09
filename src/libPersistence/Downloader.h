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

#ifndef ZILLIQA_SRC_LIBPERSISTENCE_DOWNLOADER_H_
#define ZILLIQA_SRC_LIBPERSISTENCE_DOWNLOADER_H_

#include "common/Common.h"

#include "google/cloud/storage/client.h"

#include <boost/thread/executors/basic_thread_pool.hpp>
#include <boost/thread/future.hpp>

#include <filesystem>
#include <optional>

namespace gcs = ::google::cloud::storage;

namespace zil {
namespace persistence {

class Downloader {
 public:
  ~Downloader() noexcept;

  template <typename StoragePathT, typename BucketNameT, typename TestnetNameT>
  Downloader(StoragePathT&& storagePath, BucketNameT&& bucketName,
             TestnetNameT&& testnetName, unsigned int threadCount)
      : m_storagePath{std::forward<StoragePathT>(storagePath)},
        m_bucketName{std::forward<BucketNameT>(bucketName)},
        m_testnetName{std::forward<TestnetNameT>(testnetName)},
        m_threadPool{threadCount} {}

  void Start();

 private:
  const std::filesystem::path m_storagePath;
  const std::string m_bucketName;
  const std::string m_testnetName;

  mutable gcs::Client m_client;
  boost::executors::basic_thread_pool m_threadPool;

  std::string PersistenceURLPrefix() const {
    return "incremental/" + m_testnetName + '/';
  }

  std::string StateDeltaURLPrefix() const {
    return "statedelta/" + m_testnetName + '/';
  }

  auto StoragePath() const { return m_storagePath; }
  auto PersistencePath() const { return m_storagePath / "persistence"; }
  auto PersistenceDiffPath() const { return m_storagePath / "persistenceDiff"; }
  auto StateDeltaPath() const { return m_storagePath / "StateDeltaFromS3"; }

  bool IsUploadOngoing() const;
  std::optional<uint64_t> GetCurrentTxBlkNum() const;
  void DownloadPersistenceAndStateDeltas();
  void DownloadDiffs(uint64_t fromTxBlk, uint64_t toTxBlk,
                     const std::string& prefix,
                     const std::string& fileNamePrefix,
                     const std::filesystem::path& downloadPath);
  void DownloadPersistenceDiff(uint64_t fromTxBlk, uint64_t toTxBlk);
  void DownloadStateDeltaDiff(uint64_t fromTxBlk, uint64_t toTxBlk);
  std::vector<gcs::ListObjectsReader::value_type> RetrieveBucketObjects(
      const std::string& prefix);

  using DownloadFutures = std::vector<boost::future<
      std::pair<std::string, std::optional<std::filesystem::path> > > >;

  DownloadFutures DownloadBucketObjects(
      const std::vector<gcs::ListObjectsReader::value_type>& bucketObjects,
      const std::filesystem::path& outputPath);
};

}  // namespace persistence
}  // namespace zil

#endif  // ZILLIQA_SRC_LIBPERSISTENCE_DOWNLOADER_H_
