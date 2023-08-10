#include "libPersistence/Downloader.h"

#include <archive.h>
#include <archive_entry.h>
#include <crc32c/crc32c.h>

#include <boost/algorithm/string.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>

#include <fstream>
#include <regex>

namespace zil {
namespace persistence {

namespace {
const constexpr std::chrono::seconds WAIT_INTERVAL{2};
const constexpr std::size_t FILE_CHUNK_SIZE_BYTES{512 * 1024};

std::string Decode64(const std::string& val) {
  using namespace boost::archive::iterators;
  using It =
      transform_width<binary_from_base64<std::string::const_iterator>, 8, 6>;
  return boost::algorithm::trim_right_copy_if(
      std::string(It(std::begin(val)), It(std::end(val))),
      [](char c) { return c == '\0'; });
}

void PrintTarError(int result, archive* ar) {
  assert(ar);
  if (result <= ARCHIVE_FATAL) {
    std::cerr << "Fatal: " << archive_error_string(ar) << std::endl;
  } else if (result <= ARCHIVE_FAILED) {
    std::cerr << "Error: " << archive_error_string(ar) << std::endl;
  } else if (result < ARCHIVE_OK) {
    std::cout << "Warning: " << archive_error_string(ar) << std::endl;
  }
}

int WriteEntry(archive* inArchive, archive* outArchive) {
  while (true) {
    const void* buff = nullptr;
    std::size_t size = 0;
    la_int64_t offset = 0;
    auto result = archive_read_data_block(inArchive, &buff, &size, &offset);
    if (result == ARCHIVE_EOF) {
      return ARCHIVE_OK;
    }
    if (result < ARCHIVE_OK) {
      PrintTarError(result, inArchive);
      return result;
    }

    result = archive_write_data_block(outArchive, buff, size, offset);
    if (result < ARCHIVE_OK) {
      PrintTarError(result, outArchive);
      return result;
    }
  }
}

void Extract(const std::filesystem::path& filePath) {
  auto inArchiveDeleter = [](archive* ptr) {
    archive_read_close(ptr);
    archive_read_free(ptr);
  };
  std::unique_ptr<archive, decltype(inArchiveDeleter)> inArchive{
      archive_read_new(), inArchiveDeleter};

  archive_read_support_format_all(inArchive.get());
  archive_read_support_filter_all(inArchive.get());

  auto outArchiveDeleter = [](archive* ptr) {
    archive_write_close(ptr);
    archive_write_free(ptr);
  };
  std::unique_ptr<archive, decltype(outArchiveDeleter)> outArchive{
      archive_write_disk_new(), outArchiveDeleter};

  archive_write_disk_set_options(
      outArchive.get(), ARCHIVE_EXTRACT_TIME | ARCHIVE_EXTRACT_PERM |
                            ARCHIVE_EXTRACT_ACL | ARCHIVE_EXTRACT_FFLAGS);
  archive_write_disk_set_standard_lookup(outArchive.get());

  const constexpr std::size_t BLOCK_SIZE = 10240;
  if (archive_read_open_filename(inArchive.get(), filePath.c_str(),
                                 BLOCK_SIZE) != 0) {
    std::cerr << "Failed to open file " << filePath << std::endl;
    return;
  }

  while (true) {
    archive_entry* entry = nullptr;
    auto result = archive_read_next_header(inArchive.get(), &entry);
    PrintTarError(result, inArchive.get());
    if (result == ARCHIVE_EOF) {
      return;
    }

    if (result < ARCHIVE_WARN) {
      std::cerr << "Extraction of " << filePath << " aborted!" << std::endl;
      return;
    }

    result = archive_write_header(outArchive.get(), entry);
    PrintTarError(result, outArchive.get());
    if (result >= ARCHIVE_OK && archive_entry_size(entry) > 0) {
      result = WriteEntry(inArchive.get(), outArchive.get());
      if (result < ARCHIVE_WARN) {
        std::cerr << "Extraction of " << filePath << " aborted!" << std::endl;
        return;
      }
    }
    result = archive_write_finish_entry(outArchive.get());
    PrintTarError(result, outArchive.get());
    if (result < ARCHIVE_WARN) {
      std::cerr << "Extraction of " << filePath << " aborted!" << std::endl;
      return;
    }
  }
}

void ExtractGZippedFiles(const std::filesystem::path& dirPath) {
  // The doesn't seem to be a way to tell libarchive where to extract
  // the tar to, so we need to change the current directory to make
  // sure it's written where we want.
  std::error_code errorCode;
  std::filesystem::current_path(dirPath, errorCode);

  std::vector<std::filesystem::directory_entry> dirEntries;
  std::copy_if(std::filesystem::directory_iterator(dirPath),
               std::filesystem::directory_iterator(),
               std::back_inserter(dirEntries),
               [](const auto& dirEntry) { return dirEntry.is_regular_file(); });

  for (const auto& file : dirEntries) {
    const auto& filePath = file.path();
    if (filePath.string().ends_with("tar.gz")) {
      Extract(filePath);
    }

    std::filesystem::remove(filePath);
  }
}

std::optional<std::filesystem::path> DownloadBucketObject(
    gcs::Client client, const std::string& bucketName,
    const std::string& objectName, const std::filesystem::path& outputPath,
    const std::string& expectedCrc32c) {
  auto inputStream = client.ReadObject(bucketName, objectName);
  if (!inputStream) {
    std::cerr << "Can't download bucket object (" << objectName << ") in "
              << bucketName << std::endl;
  }

  auto filePath = outputPath / objectName;
  if (!filePath.has_filename()) {
    std::cerr << "Can't infer file name for " << objectName << " in bucket "
              << bucketName << "; skipping..." << std::endl;
    return std::nullopt;
  }

  // Skip the testnet name part in the URL path
  auto fileName = filePath.filename();
  filePath = filePath.parent_path();
  filePath = filePath.has_parent_path() ? filePath.parent_path() : outputPath;
  filePath /= fileName;

  std::error_code errorCode;
  std::filesystem::create_directories(filePath.parent_path(), errorCode);
  std::ofstream outputStream{filePath, std::ios_base::binary};
  if (!outputStream) {
    std::cerr << "Can't open " << filePath << " for writing; skipping..."
              << std::endl;
    return std::nullopt;
  }

  // Calculate the CRC32c (Google's recommended validation algorithm) and make
  // sure we get the same value.
  uint32_t crc32c = 0;
  std::array<char, FILE_CHUNK_SIZE_BYTES> chunk = {};
  while (inputStream) {
    inputStream.read(chunk.data(), chunk.size());
    auto bytesRead = inputStream.gcount();
    if (bytesRead <= 0) {
      break;
    }

    crc32c = crc32c::Extend(
        crc32c, reinterpret_cast<const uint8_t*>(chunk.data()), bytesRead);
    outputStream.write(chunk.data(), bytesRead);
  }

  auto decodedCrc32c = Decode64(expectedCrc32c);
  if (decodedCrc32c.size() != sizeof(crc32c) ||
      !std::equal(std::rbegin(decodedCrc32c), std::rend(decodedCrc32c),
                  reinterpret_cast<char*>(&crc32c))) {
    std::cerr << "CRC32C mismatch for " << objectName << " in " << bucketName
              << "; skipping..." << std::endl;
    return std::nullopt;
  }

  return filePath;
}

/*
  When the DS epoch crossover happens, currTxBlk and newTxBlk will be from
  different DSepoch. As per the current behavior, Persistence is overwritten
  after every NUM_DSBLOCK * NUM_FINAL_BLOCK_PER_POW. This function ensures that
  if currTxBlk DS epoch is different from the persistence overwritten DS epoch,
  then the node restart the download again. If we don't restart the download, In
  such a case, the node will receive 404 during persistence download and the
  node can get leveldb related issues.
*/
bool IsDownloadRestartRequired(uint64_t currTxBlk, uint64_t latestTxBlk,
                               unsigned int NUM_DSBLOCK,
                               unsigned int NUM_FINAL_BLOCK_PER_POW) {
#if 0
    // print("currTxBlk = "+ str(currTxBlk) + " latestTxBlk = "+ str(latestTxBlk) + " NUM_DSBLOCK = " +str(NUM_DSBLOCK))
    if((latestTxBlk // (NUM_DSBLOCK * NUM_FINAL_BLOCK_PER_POW)) != (currTxBlk // (NUM_DSBLOCK * NUM_FINAL_BLOCK_PER_POW))):
        return true;
#endif
  return false;
}

}  // namespace

Downloader::~Downloader() noexcept {
  std::cout << "Waiting for all threads to finish..." << std::endl;
  m_threadPool.close();
  m_threadPool.join();
}

void Downloader::Start() {
  // TODO: download static DB if not excluded
  DownloadStaticDb();

  while (true) {
    if (IsUploadOngoing()) {
      std::cout << "Waiting for persistence upload to finish..." << std::endl;
      std::this_thread::sleep_for(WAIT_INTERVAL);
      continue;
    }

    auto currentTxBlk = GetCurrentTxBlkNum();
    if (!currentTxBlk) {
      std::cerr << "No current Tx block found..." << std::endl;
      std::this_thread::sleep_for(WAIT_INTERVAL);
      continue;
    }

    std::cout << "Current Tx block: " << *currentTxBlk << std::endl;
    DownloadPersistenceAndStateDeltas();

    auto newTxBlk = GetCurrentTxBlkNum();
    if (!newTxBlk || *newTxBlk < *currentTxBlk) {
      std::cerr << "Inconsistent Tx block numbers; quitting..." << std::endl;
      return;
    }

    if (*newTxBlk == *currentTxBlk) {
      return;
    }

    // TODO: retrieve NUM_DSBLOCK & NUM_FINAL_BLOCK_PER_POW
    if (IsDownloadRestartRequired(*currentTxBlk, *newTxBlk, 0, 0)) {
      std::cout << "Redownload persistence as the persistence is overwritten"
                << std::endl;
      continue;
    }

    DownloadPersistenceDiff(*currentTxBlk + 1, *newTxBlk + 1);
    DownloadStateDeltaDiff(*currentTxBlk + 1, *newTxBlk + 1);
  }
}

bool Downloader::IsUploadOngoing() const {
  auto metadata = m_client.GetObjectMetadata(m_bucketName,
                                             PersistenceURLPrefix() + ".lock");
  return metadata && metadata.status().code() == google::cloud::StatusCode::kOk;
}

std::optional<uint64_t> Downloader::GetCurrentTxBlkNum() const try {
  std::optional<uint64_t> result;

  auto stream = m_client.ReadObject(m_bucketName,
                                    PersistenceURLPrefix() + ".currentTxBlk");
  if (!stream) {
    return result;
  }

  std::string value;
  stream >> value;

  std::size_t index = 0;
  result = std::stoul(value, &index);
  if (index == value.size()) {
    // Check the content isn't negative (which would still make
    // stoul succeed).
    for (index = 0; index < value.size(); ++index) {
      if (!std::isspace(value[index])) break;
    }

    if (index < value.size() && value[index] == '-') {
      result = std::nullopt;
    }
  }

  return result;
} catch (std::exception& e) {
  std::cerr << e.what() << std::endl;
  return std::nullopt;
}

void Downloader::DownloadStaticDb() {
  //
  std::error_code errorCode;
  std::filesystem::create_directories(StaticDbPath(), errorCode);

  auto bucketObjects =
      RetrieveBucketObjects(StatidDbURLPrefix() + m_testnetName + "tar.gz");
  assert(bucketObjects.size() <= 1);
  auto staticDbFutures = DownloadBucketObjects(bucketObjects, StaticDbPath());
  boost::wait_for_all(std::ranges::begin(staticDbFutures),
                      std::ranges::end(staticDbFutures));
  ExtractGZippedFiles(StaticDbPath());
}

void Downloader::DownloadPersistenceAndStateDeltas() {
  std::error_code errorCode;
  std::filesystem::remove(PersistencePath(), errorCode);
  std::filesystem::remove(PersistenceDiffPath(), errorCode);
  std::filesystem::create_directories(StoragePath(), errorCode);

  auto bucketObjects = RetrieveBucketObjects(PersistenceURLPrefix());
  auto persistenceFutures = DownloadBucketObjects(bucketObjects, StoragePath());

  std::filesystem::remove(StateDeltaPath(), errorCode);
  std::filesystem::create_directories(StateDeltaPath(), errorCode);
  bucketObjects = RetrieveBucketObjects(StateDeltaURLPrefix());
  auto stateDeltaFutures =
      DownloadBucketObjects(bucketObjects, StateDeltaPath());

  // Wait for state deltas to finish downloading first before
  // extracting the tar.gz downloaded files.
  boost::wait_for_all(std::ranges::begin(stateDeltaFutures),
                      std::ranges::end(stateDeltaFutures));
  ExtractGZippedFiles(StateDeltaPath());

  boost::wait_for_all(std::ranges::begin(persistenceFutures),
                      std::ranges::end(persistenceFutures));
}

void Downloader::DownloadDiffs(uint64_t fromTxBlk, uint64_t toTxBlk,
                               const std::string& prefix,
                               const std::string& fileNamePrefix,
                               const std::filesystem::path& downloadPath,
                               bool excludePersistenceDiff /*= true*/) {
  auto bucketObjects =
      RetrieveBucketObjects(prefix + fileNamePrefix, excludePersistenceDiff);
  bucketObjects.erase(
      std::remove_if(
          std::ranges::begin(bucketObjects), std::ranges::end(bucketObjects),
          [fromTxBlk, toTxBlk, &fileNamePrefix](const auto& bucketObject) {
            if (!bucketObject) return false;

            // Extract the block number from the object name and handle if it's
            // in the correct range.
            std::smatch match;
            const auto& objectName = bucketObject->name();
            if (!std::regex_match(objectName, match,
                                  std::regex("^.*\\/" + fileNamePrefix +
                                             "([0-9]+)\\.tar\\.gz$"))) {
              return false;
            }

            assert(match.size() == 2);
            auto matchIter = std::ranges::next(std::ranges::begin(match));
            auto txBlk = std::stoull(matchIter->str());
            return (txBlk >= fromTxBlk && txBlk < toTxBlk);
          }),
      std::ranges::end(bucketObjects));

  auto diffFutures = DownloadBucketObjects(bucketObjects, downloadPath);
  boost::wait_for_all(std::ranges::begin(diffFutures),
                      std::ranges::end(diffFutures));

  ExtractGZippedFiles(downloadPath);
}

void Downloader::DownloadPersistenceDiff(uint64_t fromTxBlk, uint64_t toTxBlk) {
  std::error_code errorCode;
  std::filesystem::remove(PersistenceDiffPath(), errorCode);
  std::filesystem::create_directories(PersistenceDiffPath(), errorCode);

  DownloadDiffs(fromTxBlk, toTxBlk, PersistenceURLPrefix(), "diff_persistence_",
                PersistenceDiffPath(), false);

  for (const auto& dirEntry :
       std::filesystem::directory_iterator(PersistenceDiffPath())) {
    if (!dirEntry.is_directory()) {
      continue;
    }

    std::filesystem::copy(dirEntry, PersistencePath(),
                          std::filesystem::copy_options::recursive, errorCode);
    std::cout << "Copied " << dirEntry.path() << " to " << PersistencePath()
              << " (" << errorCode << ')' << std::endl;
  }

  std::filesystem::remove_all(PersistenceDiffPath(), errorCode);
}

void Downloader::DownloadStateDeltaDiff(uint64_t fromTxBlk, uint64_t toTxBlk) {
  std::error_code errorCode;
  std::filesystem::create_directories(StateDeltaPath(), errorCode);

  DownloadDiffs(fromTxBlk, toTxBlk, StateDeltaURLPrefix(), "stateDelta_",
                StateDeltaPath());
}

std::vector<gcs::ListObjectsReader::value_type>
Downloader::RetrieveBucketObjects(const std::string& prefix,
                                  bool excludePersistenceDiff /* = true*/) {
  std::vector<gcs::ListObjectsReader::value_type> result;

  auto listObjectsReader =
      m_client.ListObjects(m_bucketName, gcs::Prefix(prefix));
  for (auto& bucketObject : listObjectsReader) {
    const auto& objectName = bucketObject->name();
    if ((excludePersistenceDiff &&
         objectName.rfind("diff_persistence") != std::string::npos) ||
        (m_excludeMicroBlocks &&
         (objectName.rfind("txEpochs") != std::string::npos ||
          objectName.rfind("txBodies") != std::string::npos ||
          objectName.rfind("microBlock") != std::string::npos ||
          objectName.rfind("minerInfo") != std::string::npos))) {
      continue;
    }

    if (!bucketObject) {
      std::cerr << "Bad bucket object (" << objectName << ") in "
                << m_bucketName << '/' + prefix << std::endl;
      continue;
    }

    result.emplace_back(std::move(bucketObject));
  }

  return result;
}

Downloader::DownloadFutures Downloader::DownloadBucketObjects(
    const std::vector<gcs::ListObjectsReader::value_type>& bucketObjects,
    const std::filesystem::path& outputPath) {
  DownloadFutures result;
  result.reserve(bucketObjects.size());
  for (const auto& bucketObject : bucketObjects) {
    if (!bucketObject) {
      std::cerr << "Can't download bucket object " << bucketObject->name()
                << "; skipping..." << std::endl;
      continue;
    }

    auto future = boost::async(
        // IMPORTANT: copy the client; this is guaranteed to be thread-safe
        // according to Google.
        m_threadPool, [client = m_client, bucketName = m_bucketName,
                       objectName = bucketObject->name(), outputPath,
                       crc32c = bucketObject->crc32c()]() mutable {
          auto filePath = DownloadBucketObject(std::move(client), bucketName,
                                               objectName, outputPath, crc32c);
          return std::make_pair(std::move(bucketName), std::move(filePath));
        });

    result.emplace_back(std::move(future));
  }

  return result;
}

}  // namespace persistence
}  // namespace zil
