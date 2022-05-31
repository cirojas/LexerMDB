#pragma once

#include <cstdint>
#include <map>
#include <string>

class ObjectFileHashBucket {
friend class ObjectFileHash;

// 2 bytes needed for key_count and local_depth, 2*8 bytes for the hash
// and 6 bytes for the id (it assumes the other 2 bytes of the id are 0x00)
// TODO: maybe 5 bytes is enough => ~1TB for object_file
static constexpr auto BYTES_FOR_ID = 6U;
static constexpr auto MAX_KEYS = (4096 - 2*sizeof(uint8_t)) / (2*sizeof(uint64_t) + BYTES_FOR_ID);
static_assert(MAX_KEYS <= UINT8_MAX, "ObjectFileHashBucket KEY_COUNT(UINT8) CAN'T REACH MAX_KEYS");

public:
    ObjectFileHashBucket(char* page) :
        hashes      (reinterpret_cast<uint64_t*>(page)),
        key_count   (reinterpret_cast<uint8_t*>(page + 2*sizeof(uint64_t)*MAX_KEYS)),
        local_depth (reinterpret_cast<uint8_t*>(page + 2*sizeof(uint64_t)*MAX_KEYS + sizeof(uint8_t))),
        ids         (reinterpret_cast<uint8_t*>(page + 2*sizeof(uint64_t)*MAX_KEYS + 2*sizeof(uint8_t))) { }

    // ObjectFileHashBucket(FileId file_id, uint_fast32_t bucket_number, ObjectFile& objecy_file);

    ~ObjectFileHashBucket() = default;

    // uint64_t get_id(const std::string& str, const uint64_t hash1, const uint64_t hash2) const;

    void create_id(uint64_t new_id,
                   const uint64_t hash1,
                   const uint64_t hash2,
                   bool* const need_split)
    {
        if (*key_count == MAX_KEYS) {
            *need_split = true;
            return;
        }

        hashes[2 * (*key_count)]     = hash1;
        hashes[2 * (*key_count) + 1] = hash2;

        write_id(new_id, *key_count);
        ++(*key_count);

        *need_split = false;
    }

private:
    uint64_t* const hashes; // each tuple is (hash1, hash2)
    uint8_t*  const key_count;
    uint8_t*  const local_depth; // used by object_file_hash
    uint8_t*  const ids;

    void write_id(const uint64_t id, const uint_fast32_t index) {
        const auto offset = BYTES_FOR_ID*index;
        for (uint_fast8_t b = 0; b < BYTES_FOR_ID; b++) {
            ids[offset + b] = static_cast<uint8_t>( (id >> (8UL*b)) & 0xFF );
        }
    }

    uint64_t read_id(const uint_fast32_t index) const {
        const auto offset = BYTES_FOR_ID * index;
        uint64_t res = 0;
        for (uint_fast8_t b = 0; b < BYTES_FOR_ID; b++) {
            res += static_cast<uint64_t>(ids[offset + b]) << 8UL*b;
        }
        return res;
    }

    void redistribute(ObjectFileHashBucket& other, const uint64_t mask, const uint64_t other_suffix) {
        uint8_t other_pos = 0;
        uint8_t this_pos = 0;

        for (uint8_t i = 0; i < *key_count; i++) {
            auto suffix = mask & hashes[2 * i];

            if (suffix == other_suffix) {
                // copy hash to other bucket
                std::memcpy(
                    &other.hashes[2*other_pos],
                    &hashes[2*i],
                    2 * sizeof(uint64_t));

                // copy id to ohter bucket
                std::memcpy(
                    &other.ids[BYTES_FOR_ID*other_pos],
                    &ids[BYTES_FOR_ID*i],
                    BYTES_FOR_ID * sizeof(uint8_t));

                ++other_pos;
            } else {
                if (i != this_pos) { // avoid redundant copy
                    // copy hash in this bucket
                    std::memcpy(
                        &hashes[2*this_pos],
                        &hashes[2*i],
                        2 * sizeof(uint64_t));

                    // copy id in this bucket
                    std::memcpy(
                        &ids[BYTES_FOR_ID*this_pos],
                        &ids[BYTES_FOR_ID*i],
                        BYTES_FOR_ID * sizeof(uint8_t));
                }
                ++this_pos;
            }
        }
        *this->key_count = this_pos;
        *other.key_count = other_pos;
    }
};
