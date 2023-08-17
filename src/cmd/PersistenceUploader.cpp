#include "libPersistence/Uploader.h"

#include <boost/program_options.hpp>

#include <iostream>

namespace po = boost::program_options;

namespace {
const constexpr std::size_t DEFAULT_THREAD_COUNT = 10;
}

int main(int argc, char* argv[]) {
  po::options_description desc("Options");

  std::string webhookUrl;
  unsigned int txBlockTime = 0;
  unsigned int dsBlockTime = 0;
  bool backup = false;
  std::filesystem::path storagePath;
  std::string bucketName;
  std::string testnetName;
  unsigned int threadCount = 0;
  desc.add_options()("help,h", "Print help messages")(
      "webhook,w", po::value<std::string>(&webhookUrl), "Slack webhook URL")(
      "txblktime,x", po::value<unsigned int>(&txBlockTime)->default_value(60),
      "Avg txBlockTime to get mined (in seconds)")(
      "dsblktime,d", po::value<unsigned int>(&dsBlockTime)->default_value(600),
      "Avg dxBlockTime to get mined (in seconds)")(
      "backup,b", po::value<bool>(&backup)->required()->default_value(true),
      "Upload to backup")(
      "storage-path,s",
      po::value<std::filesystem::path>(&storagePath)->required(),
      "The path to upload the persistence to")(
      "bucket-name,b", po::value<std::string>(&bucketName)->required(),
      "The name of the bucket")(
      "testnet-name,n", po::value<std::string>(&testnetName)->required(),
      "The name of the testnet")(
      "threads,t",
      po::value<unsigned int>(&threadCount)
          ->default_value(DEFAULT_THREAD_COUNT),
      "The (maximum) number of threads to use when uploading persistence");

  po::variables_map vm;

  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help")) {
      std::cout << desc << std::endl;
      return 0;
    }

    po::notify(vm);

    zil::persistence::Uploader uploader{std::move(webhookUrl),
                                        std::chrono::seconds{txBlockTime},
                                        std::chrono::seconds{dsBlockTime},
                                        backup,
                                        std::move(storagePath),
                                        std::move(bucketName),
                                        std::move(testnetName),
                                        threadCount};

    uploader.Start();
  } catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    return -1;
  }
}
