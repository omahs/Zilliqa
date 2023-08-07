#include "libPersistence/Downloader.h"

namespace zil {
namespace persistence {

void Downloader::start() {
  auto bucketMetadata = m_client.GetBucketMetadata(m_bucketName);
  std::cout << bucketMetadata->kind() << std::endl;

  /** --help option
   */
}

}  // namespace persistence
}  // namespace zil
