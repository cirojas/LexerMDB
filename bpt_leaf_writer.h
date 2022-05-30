#pragma once

// #include <fstream>
#include <cstring>
#include <cstdlib>
// #include <ios>
#include <fcntl.h>
#include <unistd.h>

#include <libaio.h>
#include <new>

template <std::size_t N>
class BPTLeafWriter {
public:
    static constexpr auto page_size = 4096;
    static constexpr auto max_records = (page_size - 2*sizeof(int32_t)) / (sizeof(uint64_t)*N);

    BPTLeafWriter(const std::string& filename) {
        fd = open(filename.c_str(), O_CREAT | O_WRONLY | O_DIRECT, S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH);
        // file.open(filename, std::ios::out|std::ios::binary);
        buffer = new (std::align_val_t(page_size)) char[page_size];
        // buffer = new char[page_size];
        // posix_memalign((void*)buffer, page_size, page_size);

        iocbs = &cb;
        memset(&ctx, 0, sizeof(ctx));
        if (io_setup(10, &ctx) != 0) { // init
            printf("io_setup error\n");
            // return -1;
        }
    }

    ~BPTLeafWriter() {
        int res = io_destroy(ctx);
        if (res < 0) {
            printf("io_destroy Error: %d\n", res);
        }
        close(fd);
        // file.close();
        // delete[] buffer;
        free(buffer);
    }

    void process_block(char* bytes, uint32_t size, uint32_t next_block) {
        auto value_count = reinterpret_cast<uint32_t*>(buffer);
        auto next_leaf   = reinterpret_cast<uint32_t*>(buffer + sizeof(uint32_t));

        memset(buffer, 0, page_size);
        *value_count = size;
        *next_leaf = next_block;
        std::memcpy(buffer + 2*sizeof(uint32_t), bytes, size*(N*sizeof(uint64_t)));
        // file.write(buffer, page_size);
        io_prep_pwrite(&cb, fd, &buffer, page_size, current_offset);
        int res = io_submit(ctx, 1, &iocbs);
        if (res < 0) {
            printf("io_submit error\n");
        }
        current_offset += page_size;
    }

    void make_empty() {
        memset(buffer, 0, page_size);
        // file.write(buffer, page_size);
        io_prep_pwrite(&cb, fd, &buffer, page_size, current_offset);
        int res = io_submit(ctx, 1, &iocbs);
        if (res < 0) {
            printf("io_submit error\n");
        }
        // current_offset += page_size;
    }

private:
    // std::fstream file;
    int fd;
    // iocb cb;
    char* buffer;
    size_t current_offset = 0;
    iocb iocbs[10];
    io_context_t ctx;
};
