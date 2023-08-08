#include "libPersistence/Downloader.h"

#include <boost/program_options.hpp>

#include <iostream>

namespace po = boost::program_options;

namespace {
const constexpr std::size_t DEFAULT_THREAD_COUNT = 50;
}

int main(int argc, char* argv[]) {
  po::options_description desc("Options");

  std::filesystem::path storagePath;
  std::string bucketName;
  std::string testnetName;
  unsigned int threadCount = 0;
  desc.add_options()("help,h", "Print help messages")(
      "storage-path,s",
      po::value<std::filesystem::path>(&storagePath)->required(),
      "The path to download the persistence to")(
      "bucket-name,b", po::value<std::string>(&bucketName)->required(),
      "The name of the bucket")(
      "testnet-name,n", po::value<std::string>(&testnetName)->required(),
      "The name of the testnet")(
      "threads,t",
      po::value<unsigned int>(&threadCount)
          ->default_value(DEFAULT_THREAD_COUNT),
      "The (maximum) number of threads to use when downloading persistence");

  po::variables_map vm;

  try {
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help")) {
      std::cout << desc << std::endl;
      return 0;
    }

    po::notify(vm);

    zil::persistence::Downloader downloader{
        std::move(storagePath), std::move(bucketName), std::move(testnetName),
        threadCount};

    downloader.Start();

  } catch (std::exception& e) {
    std::cerr << e.what() << std::endl;
    return -1;
  }
}
