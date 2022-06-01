#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <fstream>
#include <vector>

class ObjectFileHash {
public:
    static constexpr auto DEFAULT_GLOBAL_DEPTH = 8;

    ObjectFileHash(const std::string& filename);
    ~ObjectFileHash();

    void create_id(const char* str, uint64_t id);

private:
    std::fstream dir_file;
    std::fstream buckets_file;

    uint_fast8_t global_depth;

    // array of size 2^global_depth
    uint_fast32_t* dir;

    void duplicate_dirs();

    std::vector<char*> pages;
};
