#pragma once

#include <fstream>
#include <cstring>
#include <ios>

class BPTLeafWriter {
public:
    static constexpr auto max_records = (4096 - 8) / (sizeof(uint64_t)*4);

    BPTLeafWriter(const char* filename) {
        file.open(filename, std::ios::out|std::ios::binary);
        buffer = new char[4096];
        buffer_pos = 4;

        memset(buffer, 0, 4096);
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
        if (size < 127) {
            memset(buffer, 0, 4096);
        }
        *value_count = size;
        *next_leaf = next_block;
        std::memcpy(buffer+8, bytes, size*(4*sizeof(uint64_t)));
        file.write(buffer, 4096);
    }


private:
    std::fstream file;

    char* buffer;
    int buffer_pos;
};
