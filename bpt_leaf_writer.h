#pragma once

#include <fstream>
#include <cstring>
#include <ios>

template <std::size_t N>
class BPTLeafWriter {
public:
    static constexpr auto page_size = 4096;
    static constexpr auto max_records = (page_size - 2*sizeof(int32_t)) / (sizeof(uint64_t)*N);

    BPTLeafWriter(const char* filename) {
        file.open(filename, std::ios::out|std::ios::binary);
        buffer = new char[page_size];
        buffer_pos = 4;

        memset(buffer, 0, page_size);
    }

    ~BPTLeafWriter() {
        file.close();
        delete[] buffer;
    }

    void process_block(char* bytes, uint32_t size, uint32_t next_block) {
        // copiar desde bytes 4096-8 bytes un buffer
        // write next_leaf(4 bytes, 4 offset) and value_count(4 bytes, 0 offset)
        auto value_count = reinterpret_cast<uint32_t*>(buffer);
        auto next_leaf   = value_count + 1;
        if (size < max_records) {
            memset(buffer, 0, page_size);
        }
        *value_count = size;
        *next_leaf = next_block;
        std::memcpy(buffer + 2*sizeof(int32_t), bytes, size*(4*sizeof(uint64_t)));
        file.write(buffer, page_size);
    }


private:
    std::fstream file;

    char* buffer;
    int buffer_pos;
};
