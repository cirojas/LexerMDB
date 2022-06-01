#pragma once

#include <cstdint>
#include <map>
#include <string>

class ObjectFileHashBucket {
friend class ObjectFileHash;

// 2 bytes needed for key_count and local_depth, 2*8 bytes for the hash
// and 6 bytes for the id (it assumes the other 2 bytes of the id are 0x00)
// TODO: maybe 5 bytes is enough => ~1TB for object_file
// static constexpr auto BYTES_FOR_ID = 6U;
// static constexpr auto MAX_KEYS = (4096 - 2*sizeof(uint8_t)) / (2*sizeof(uint64_t) + BYTES_FOR_ID);
static constexpr auto MAX_KEYS = (4096 - sizeof(uint64_t) ) / (2*sizeof(uint64_t));
static_assert(MAX_KEYS <= UINT8_MAX, "ObjectFileHashBucket KEY_COUNT(UINT8) CAN'T REACH MAX_KEYS");

public:
    ObjectFileHashBucket(char* page) :
        key_count   (reinterpret_cast<uint32_t*>(page)),
        local_depth (reinterpret_cast<uint32_t*>(page + sizeof(uint32_t))),
        hashes      (reinterpret_cast<uint64_t*>(page + 2*sizeof(uint32_t))),
        ids         (reinterpret_cast<uint64_t*>(page + sizeof(uint64_t) + sizeof(uint64_t)*MAX_KEYS)) { }

    void create_id(uint64_t new_id, const uint64_t hash, bool* const need_split)
    {
        if (*key_count == MAX_KEYS) {
            *need_split = true;
            return;
        }

        hashes[*key_count] = hash;
        ids[*key_count] = new_id;
        ++(*key_count);

        *need_split = false;
    }

private:
    uint32_t*  const key_count;
    uint32_t*  const local_depth; // used by object_file_hash
    uint64_t* const hashes;
    uint64_t* const ids;

    // void write_id(const uint64_t id, size_t index) {
    //     ids[index] = id;
    // }

    uint64_t read_id(size_t index) const {
        return ids[index];
    }

    void redistribute(ObjectFileHashBucket& other, const uint64_t mask, const uint64_t other_suffix) {
        uint32_t other_pos = 0;
        uint32_t this_pos = 0;

        for (size_t i = 0; i < *key_count; i++) {
            auto suffix = mask & hashes[i];

            if (suffix == other_suffix) {
                other.hashes[other_pos] = hashes[i];
                other.ids[other_pos] = ids[i];

                ++other_pos;
            } else {
                hashes[this_pos] = hashes[i];
                ids[this_pos] = ids[i];

                ++this_pos;
            }
        }
        *this->key_count = this_pos;
        *other.key_count = other_pos;
    }
};
