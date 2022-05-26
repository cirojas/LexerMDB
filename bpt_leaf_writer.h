#pragma once

#include <fstream>
#include <cstring>
#include <ios>

template <std::size_t N>
class BPTLeafWriter {
public:
    static constexpr auto page_size = 4096;
    static constexpr auto max_records = (page_size - 2*sizeof(int32_t)) / (sizeof(uint64_t)*N);

    BPTLeafWriter(const std::string& filename) {
        file.open(filename, std::ios::out|std::ios::binary);
        buffer = new char[page_size];
    }

    ~BPTLeafWriter() {
        file.close();
        delete[] buffer;
    }

    void process_block(char* bytes, uint32_t size, uint32_t next_block) {
        auto value_count = reinterpret_cast<uint32_t*>(buffer);
        auto next_leaf   = reinterpret_cast<uint32_t*>(buffer + sizeof(uint32_t));

        memset(buffer, 0, page_size);
        *value_count = size;
        *next_leaf = next_block;
        std::memcpy(buffer + 2*sizeof(uint32_t), bytes, size*(N*sizeof(uint64_t)));
        file.write(buffer, page_size);
    }

    void make_empty() {
        memset(buffer, 0, page_size);
        file.write(buffer, page_size);
    }

private:
    std::fstream file;

    char* buffer;
};
