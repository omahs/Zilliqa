#include "libPersistence/Downloader.h"

#include <archive.h>
#include <archive_entry.h>
#include <crc32c/crc32c.h>

#include <boost/algorithm/string.hpp>
#include <boost/archive/iterators/binary_from_base64.hpp>
#include <boost/archive/iterators/transform_width.hpp>

#include <fstream>

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

void DownloadBucketObject(gcs::Client client, const std::string& bucketName,
                          const std::string& objectName,
                          const std::filesystem::path& outputPath,
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
    return;
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
    return;
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
    return;
  }
}

}  // namespace

Downloader::~Downloader() noexcept {
  std::cout << "Waiting for all threads to finish..." << std::endl;
  m_threadPool.close();
  m_threadPool.join();
}

void Downloader::Start() {
  // TODO: download static DB

  while (IsUploadOngoing()) {
    std::cout << "Waiting for persistence upload to finish..." << std::endl;
    std::this_thread::sleep_for(WAIT_INTERVAL);
  }

  std::optional<uint64_t> currentTxBlk;
  for (currentTxBlk = GetCurrentTxBlkNum(); !currentTxBlk;
       currentTxBlk = GetCurrentTxBlkNum()) {
    std::cerr << "No current Tx block found..." << std::endl;
    std::this_thread::sleep_for(WAIT_INTERVAL);
  }

  std::cout << "Current Tx block: " << *currentTxBlk << std::endl;
  DownloadPersistenceAndStateDeltas();
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

void Downloader::DownloadPersistenceAndStateDeltas() {
  std::error_code errorCode;
  std::filesystem::remove(PersistencePath(), errorCode);
  std::filesystem::remove(PersistenceDiffPath(), errorCode);
  std::filesystem::create_directories(StoragePath(), errorCode);

  auto bucketObjects = RetrieveBucketObjects(PersistenceURLPrefix());
  DownloadBucketObjects(bucketObjects, StoragePath());

  std::filesystem::remove(StateDeltaPath(), errorCode);
  std::filesystem::create_directories(StateDeltaPath(), errorCode);
  bucketObjects = RetrieveBucketObjects(StateDeltaURLPrefix());
  DownloadBucketObjects(bucketObjects, StateDeltaPath());
  ExtractGZippedFiles(StateDeltaPath());
}

std::vector<gcs::ListObjectsReader::value_type>
Downloader::RetrieveBucketObjects(const std::string& url) {
  auto listObjectsReader =
      m_client.ListObjects(m_bucketName, gcs::Prefix(PersistenceURLPrefix()));

  std::vector<gcs::ListObjectsReader::value_type> bucketObjects;
  for (auto& bucketObject : listObjectsReader) {
    if (!bucketObject) {
      std::cerr << "Bad bucket object (" << bucketObject->name() << ") in "
                << m_bucketName << '/' + PersistenceURLPrefix() << std::endl;
      continue;
    }

    // TODO: exclude if (not (Exclude_txnBodies and "txEpochs" in key_url) and
    // not (Exclude_txnBodies and "txBodies" in key_url) and not
    // (Exclude_microBlocks and "microBlock" in key_url) and not
    // (Exclude_minerInfo and "minerInfo" in key_url) and not
    // ("diff_persistence" in key_url)):
    bucketObjects.emplace_back(std::move(bucketObject));
  }

  return bucketObjects;
}

void Downloader::DownloadBucketObjects(
    const std::vector<gcs::ListObjectsReader::value_type>& bucketObjects,
    const std::filesystem::path& outputPath) {
  for (const auto& bucketObject : bucketObjects) {
    if (!bucketObject) {
      std::cerr << "Can't download bucket object " << bucketObject->name()
                << "; skipping..." << std::endl;
      continue;
    }

    // IMPORTANT: copy the client; this is guaranteed to be thread-safe
    // according to Google.
    m_threadPool.submit([client = m_client, bucketName = m_bucketName,
                         objectName = bucketObject->name(), outputPath,
                         crc32c = bucketObject->crc32c()]() mutable {
      DownloadBucketObject(std::move(client), bucketName, objectName,
                           outputPath, crc32c);
    });
  }
}

}  // namespace persistence
}  // namespace zil
