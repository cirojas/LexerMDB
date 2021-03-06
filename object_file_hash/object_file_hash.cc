#include "object_file_hash.h"

#include <bitset>
#include <cassert>
#include <cmath>
#include <cstring>
#include <iostream>

#include "object_file_hash_bucket.h"
#include "../murmur3/murmur3.h"


ObjectFileHash::ObjectFileHash(const std::string& filename) {
    dir_file.open(filename + ".dir", std::ios::out|std::ios::binary);
    buckets_file.open(filename + ".dat", std::ios::out|std::ios::binary);
    dir_file.seekg(0, dir_file.end);

    global_depth = DEFAULT_GLOBAL_DEPTH;
    uint_fast32_t dir_size = 1 << global_depth;
    dir = new uint_fast32_t[dir_size];
    for (uint_fast32_t i = 0; i < dir_size; ++i) {
        auto new_page = new char[4096];
        memset(new_page, 0, 4096);
        pages.push_back(new_page);
        ObjectFileHashBucket bucket(new_page);
        *bucket.key_count = 0;
        *bucket.local_depth = DEFAULT_GLOBAL_DEPTH;
        dir[i] = i;
    }
}


ObjectFileHash::~ObjectFileHash() {
    dir_file.seekg(0, dir_file.beg);

    dir_file.write(reinterpret_cast<const char*>(&global_depth), sizeof(uint8_t));
    uint_fast32_t dir_size = 1 << global_depth;
    for (uint64_t i = 0; i < dir_size; ++i) {
        uint32_t tmp = dir[i];
        dir_file.write(reinterpret_cast<const char*>(&tmp), sizeof(tmp));
    }
    delete[](dir);
    dir_file.close();

    buckets_file.seekg(0, buckets_file.beg);
    for (auto page : pages) {
        buckets_file.write(page, 4096);
        delete[](page);
    }
    buckets_file.close();
}


void ObjectFileHash::duplicate_dirs() {
    uint64_t old_dir_size = 1UL << global_depth;
    ++global_depth;
    auto new_dir_size = 1UL << global_depth;
    auto new_dir = new uint_fast32_t[new_dir_size];

    std::memcpy(new_dir,
                dir,
                old_dir_size * sizeof(uint_fast32_t));

    std::memcpy(&new_dir[old_dir_size],
                dir,
                old_dir_size * sizeof(uint_fast32_t));

    delete[](dir);
    dir = new_dir;

    // for (uint_fast32_t i = old_dir_size; i < new_dir_size; ++i) {
    //     auto new_page = new char[4096];
    //     memset(new_page, 0, 4096);
    //     pages.push_back(new_page);
    //     ObjectFileHashBucket bucket(new_page);
    // }
}


void ObjectFileHash::create_id(const char* str, uint64_t id) {
    uint64_t hash[2];
    MurmurHash3_x64_128(str, strlen(str), 0, hash);

    // After a bucket split, need to try insert again.
    while (true) {
        // global_depth must be <= 64
        auto mask = 0xFFFF'FFFF'FFFF'FFFF >> (64 - global_depth);
        auto suffix = hash[0] & mask;
        // auto bucket_number = dir[suffix]; // TODO: no usar bucket_number? sino suffix
        ObjectFileHashBucket bucket(pages[dir[suffix]]);

        bool need_split;
        bucket.create_id(id, hash[0], hash[1], &need_split);

        if (need_split) {
            auto new_page = new char[4096];
            memset(new_page, 0, 4096);
            auto new_bucket_number = pages.size();
            pages.push_back(new_page);
            ObjectFileHashBucket new_bucket(new_page);
            *new_bucket.key_count   = 0;
            *new_bucket.local_depth = *bucket.local_depth + 1;

            if (*bucket.local_depth < global_depth) {
                auto local_mask = 0xFFFF'FFFF'FFFF'FFFF >> (64 - *bucket.local_depth);
                auto new_suffix = (1 << (*bucket.local_depth)) | (local_mask & suffix); // 1|local_suffix

                ++(*bucket.local_depth);
                auto new_mask = 0xFFFF'FFFF'FFFF'FFFF >> (64 - (*bucket.local_depth));

                // redistribute keys between buckets `0|suffix` and `1|suffix`
                bucket.redistribute(new_bucket, new_mask, new_suffix);

                // update dirs having `new_suffix` as suffix and point to the new bucket number
                auto update_dir_count = 1 << (global_depth - (*bucket.local_depth));
                for (auto i = 0; i < update_dir_count; ++i) {
                    dir[(i << (*bucket.local_depth)) | new_suffix] = new_bucket_number;
                }

                // assert(*bucket.key_count + *new_bucket.key_count == ObjectFileHashBucket::MAX_KEYS
                //     && "EXTENDIBLE HASH INCONSISTENCY: sum of keys must be MAX_KEYS after a split");
                // if (*bucket.key_count + *new_bucket.key_count != ObjectFileHashBucket::MAX_KEYS) {
                //     std::cout << "A"  << (int)*bucket.key_count << '+' << (int)*new_bucket.key_count << '=' << (*bucket.key_count + *new_bucket.key_count) << std::endl;
                // }

            } else {
                // assert(*bucket.local_depth == global_depth && "EXTENDIBLE HASH INCONSISTENCY: *bucket.local_depth != global_depth");
                ++(*bucket.local_depth);

                auto new_suffix = (1 << global_depth) | suffix; // 1|suffix

                duplicate_dirs(); // increases global_depth

                // redistribute keys between buckets `0|suffix` and `1|suffix`
                auto new_mask = 0xFFFF'FFFF'FFFF'FFFF >> (64 - global_depth);
                bucket.redistribute(new_bucket, new_mask, new_suffix);

                // update dir for `1|suffix`
                dir[new_suffix] = new_bucket_number;

                // assert(*bucket.key_count + *new_bucket.key_count == ObjectFileHashBucket::MAX_KEYS
                //     && "EXTENDIBLE HASH INCONSISTENCY: sum of keys must be MAX_KEYS after a split");
                // if (*bucket.key_count + *new_bucket.key_count != ObjectFileHashBucket::MAX_KEYS) {
                //     std::cout << "B" << (int)*bucket.key_count << '+' << (int)*new_bucket.key_count << '=' << (*bucket.key_count + *new_bucket.key_count) << std::endl;
                // }
            }
        } else {
            return;
        }
    }
}
