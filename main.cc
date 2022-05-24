// #define XXH_STATIC_LINKING_ONLY   /* access advanced declarations */
// #define XXH_IMPLEMENTATION   /* access definitions */
#define XXH_INLINE_ALL  // TODO: poner en xxhash/xxhash.h ?
#define XXH_VECTOR XXH_AVX2

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <iostream>
// #include <cerrno>
// #include <unordered_set>
// #include <unordered_map>
// #include <thread>
#include <vector>

// #include <sys/mman.h>

#include "bpt_leaf_writer.h"
#include "bpt_dir_writer.h"
#include "inliner.h"
#include "lexer.h"
#include "object_id.h"
#include "robin_hood.h"
#include "state.h"
#include "xxhash/xxhash.h"
// #include "murmur3/murmur3.h"

// Estados:
constexpr int STATES = 14; // TODO: extraer
constexpr int TOKENS = 16; // TODO: extraer
int* state_transitions;
void (*state_funcs [TOKENS*STATES])();
Lexer lexer;
int current_line = 1;

int label_count = 0;
int property_count = 0;
int node_count = 0;
int edge_count = 0;

uint64_t id1;
uint64_t id2;
uint64_t edge_id = 0;
// uint64_t anon_id = 0;

uint64_t type_id;
uint64_t key_id;

// true if right
bool direction;

std::vector<std::array<uint64_t, 2>> labels;
std::vector<std::array<uint64_t, 3>> properties;
std::vector<std::array<uint64_t, 4>> edges;

// uint64_t external_strings_capacity = 4096*1024;
size_t external_strings_initial_size = 1024ULL * 1024ULL * 1024ULL * 1ULL;
char* external_strings = new char[external_strings_initial_size];
// char* external_strings = (char*) mmap(
//     NULL,
//     mmap_size,
//     PROT_READ|PROT_WRITE,
//     MAP_ANON|MAP_SHARED,
//     -1,
//     0);

uint64_t external_strings_capacity = external_strings_initial_size;
uint64_t external_strings_end = 1;

class ExternalString {
public:
    static char* strings;
    uint64_t offset;

    ExternalString(uint64_t offset) : offset(offset) { }

    bool operator==(const ExternalString& other) const {
        return strcmp(strings + offset, strings + other.offset) == 0;
    }
};

char* ExternalString::strings = nullptr;

struct ExternalStringHasher {
    size_t operator()(const ExternalString& str) const {
        size_t str_len = strlen(ExternalString::strings + str.offset);
        return XXH3_64bits(ExternalString::strings + str.offset, str_len);
    }
};

void check_external_string_size() {
    while (external_strings_end + lexer.str_len + 1 >= external_strings_capacity) {
        // duplicate buffer
        char* new_external_strings = new char[external_strings_capacity*2];
        std::memcpy(new_external_strings,
                    external_strings,
                    external_strings_capacity);

        external_strings_capacity *= 2;

        delete[] external_strings;
        external_strings = new_external_strings;
        ExternalString::strings = external_strings;
    }
}

robin_hood::unordered_set<ExternalString, ExternalStringHasher> external_ids_map;

uint64_t get_or_create_external_string_id() {
    check_external_string_size();
    std::memcpy(
        &external_strings[external_strings_end],
        lexer.str,
        lexer.str_len);
    external_strings[external_strings_end + lexer.str_len] = '\0';
    ExternalString s(external_strings_end);

    auto search = external_ids_map.find(s);
    if (search == external_ids_map.end()) {
        external_strings_end += lexer.str_len + 1;
        external_ids_map.insert(s);
        return s.offset;
    } else {
        return search->offset;
    }
}

// modifies conetents of lexer.str and lexer.str_len. lexer.str points to the same place
void normalize_string_literal() {
    // std::cout << "original: " << lexer.str;
    char* write_ptr = lexer.str;
    char* read_ptr = write_ptr + 1; // skip first character: '"'

    lexer.str_len -= 2;
    char* end = lexer.str + lexer.str_len + 1;

    while (read_ptr < end) {
        if (*read_ptr == '\\') {
            read_ptr++;
            lexer.str_len--;
            switch (*read_ptr) {
                case '"':
                    *write_ptr = '"';
                    break;
                case '\\':
                    *write_ptr = '\\';
                    break;
                case 'n':
                    *write_ptr = '\n';
                    break;
                case 't':
                    *write_ptr = '\t';
                    break;
                case 'r':
                    *write_ptr = '\r';
                    break;
                // case 'f':
                //     *write_ptr = '\f';
                //     break;
                // case 'b':
                //     *write_ptr = '\b';
                //     break;
                default:
                    *write_ptr = '\\';
                    write_ptr++;
                    *write_ptr = *read_ptr;
                    lexer.str_len++;
                    break;
            }
            read_ptr++;
            write_ptr++;

        } else {
            *write_ptr = *read_ptr;
            read_ptr++;
            write_ptr++;
        }
    }
    *write_ptr = '\0';
    // std::cout << "\nafter: " << lexer.str;
    // std::cout << "\nstrlen: " << lexer.str_len;
    // exit(0);
}

const char* decode_id(uint64_t id) {
    auto type_mask = id & ObjectId::TYPE_MASK;
    auto value_mask = id & ObjectId::VALUE_MASK;

    if (type_mask == ObjectId::IDENTIFIABLE_EXTERNAL_MASK) {
        return &external_strings[value_mask];
    } else {
        char* c = new char[8];
        int shift_size = 6*8;
        for (int i = 0; i < ObjectId::MAX_INLINED_BYTES; i++) {
            uint8_t byte = (id >> shift_size) & 0xFF;
            c[i] = byte;
            shift_size -= 8;
        }
        c[7] = '\0';
        return c;
    }
}

void do_nothing() { }
void set_left_direction() { direction = false; }
void set_right_direction() { direction = true; }

void save_first_id_identifier() {
    if (lexer.str_len < 8) {
        id1 = Inliner::inline_string(lexer.str) | ObjectId::IDENTIFIABLE_INLINED_MASK ;
    } else {
        id1 = get_or_create_external_string_id() | ObjectId::IDENTIFIABLE_EXTERNAL_MASK;
    }
}

void save_first_id_anon() {
    uint64_t unmasked_id = std::stoull(lexer.str + 2);
    id1 = unmasked_id | ObjectId::ANONYMOUS_NODE_MASK;
}

void save_first_id_string() {
    normalize_string_literal();

    if (lexer.str_len < 8) {
        id1 = Inliner::inline_string(lexer.str) | ObjectId::VALUE_INLINE_STR_MASK ;
    } else {
        id1 = get_or_create_external_string_id() | ObjectId::VALUE_EXTERNAL_STR_MASK;
    }
}

void save_first_id_iri() {
    //TODO:
}

void save_first_id_int() {
    id1 = Inliner::inline_int(atoll(lexer.str));
}

void save_first_id_float() {
    id1 = Inliner::inline_float(atof(lexer.str));
}

void save_first_id_true() {
    id1 = ObjectId::VALUE_BOOL_MASK | 0x01;
}

void save_first_id_false() {
    id1 = ObjectId::VALUE_BOOL_MASK | 0x00;
}

void save_first_id_implicit() {
    // TODO:
}

void save_edge_type() {
    if (lexer.str_len < 8) {
        type_id = Inliner::inline_string(lexer.str) | ObjectId::IDENTIFIABLE_INLINED_MASK ;
    } else {
        type_id = get_or_create_external_string_id() | ObjectId::IDENTIFIABLE_EXTERNAL_MASK;
    }
    edge_id++;
    if (direction) {
        edges.push_back({id1, id2, type_id, edge_id | ObjectId::CONNECTION_MASK});
    } else {
        edges.push_back({id2, id1, type_id, edge_id | ObjectId::CONNECTION_MASK});
    }
}


void save_prop_key() {
    if (lexer.str_len < 8) {
        key_id = Inliner::inline_string(lexer.str) | ObjectId::VALUE_INLINE_STR_MASK ;
    } else {
        key_id = get_or_create_external_string_id() | ObjectId::VALUE_EXTERNAL_STR_MASK  ;
    }
}

// void save_first_id() { id1 = lexer.str; }

void save_second_id_identifier() {
    if (lexer.str_len < 8) {
        id2 = Inliner::inline_string(lexer.str) | ObjectId::IDENTIFIABLE_INLINED_MASK ;
    } else {
        id2 = get_or_create_external_string_id() | ObjectId::IDENTIFIABLE_EXTERNAL_MASK;
    }
}

void save_second_id_anon() {
    // delete first 2 characters: '_a'
    uint64_t unmasked_id = std::stoull(lexer.str + 2);
    id2 = unmasked_id | ObjectId::ANONYMOUS_NODE_MASK;
}

void save_second_id_string() {
    normalize_string_literal();

    if (lexer.str_len < 8) {
        id2 = Inliner::inline_string(lexer.str) | ObjectId::VALUE_INLINE_STR_MASK ;
    } else {
        id2 = get_or_create_external_string_id() | ObjectId::VALUE_EXTERNAL_STR_MASK;
    }
}

void save_second_id_iri() {
    // TODO:
}

void save_second_id_int() {
    id2 = Inliner::inline_int(atoll(lexer.str));
}

void save_second_id_float() {
    id2 = Inliner::inline_float(atof(lexer.str));
}

void save_second_id_true() {
    id2 = ObjectId::VALUE_BOOL_MASK | 0x01;
}

void save_second_id_false() {
    id2 = ObjectId::VALUE_BOOL_MASK | 0x00;
}

void add_node_label() {
    label_count++;
    uint64_t label_id;
    if (lexer.str_len < 8) {
        label_id = Inliner::inline_string(lexer.str) | ObjectId::VALUE_INLINE_STR_MASK ;
    } else {
        label_id = get_or_create_external_string_id() | ObjectId::VALUE_EXTERNAL_STR_MASK;
    }
    labels.push_back({id1, label_id});
}

void add_node_prop_string() {
    property_count++;

    uint64_t value_id;

    normalize_string_literal(); // edits lexer.str_len and lexer.str

    if (lexer.str_len < 8) {
        value_id = Inliner::inline_string(lexer.str) | ObjectId::VALUE_INLINE_STR_MASK ;
    } else {
        value_id = get_or_create_external_string_id() | ObjectId::VALUE_EXTERNAL_STR_MASK;
    }
    properties.push_back({id1, key_id, value_id});
}

void add_node_prop_int() {
    property_count++;
    uint64_t value_id = Inliner::inline_int(atoll(lexer.str));
    properties.push_back({id1, key_id, value_id});
}

void add_node_prop_float() {
    property_count++;
    uint64_t value_id = Inliner::inline_float(atof(lexer.str));
    properties.push_back({id1, key_id, value_id});
}

void add_node_prop_true() {
    property_count++;
    uint64_t value_id = ObjectId::VALUE_BOOL_MASK | 0x01;
    properties.push_back({id1, key_id, value_id});
}

void add_node_prop_false() {
    property_count++;
    uint64_t value_id = ObjectId::VALUE_BOOL_MASK | 0x00;
    properties.push_back({id1, key_id, value_id});
}

void add_edge_prop_string() {
    property_count++;
    uint64_t value_id;

    normalize_string_literal(); // edits lexer.str_len and lexer.str

    if (lexer.str_len < 8) {
        value_id = Inliner::inline_string(lexer.str) | ObjectId::VALUE_INLINE_STR_MASK ;
    } else {
        value_id = get_or_create_external_string_id() | ObjectId::VALUE_EXTERNAL_STR_MASK;
    }
    properties.push_back({edge_id, key_id, value_id});
}

void add_edge_prop_int() {
    property_count++;
    uint64_t value_id = Inliner::inline_int(atoll(lexer.str));
    properties.push_back({edge_id, key_id, value_id});
}

void add_edge_prop_float() {
    property_count++;
    uint64_t value_id = Inliner::inline_float(atof(lexer.str));
    properties.push_back({edge_id, key_id, value_id});
}

void add_edge_prop_true() {
    property_count++;
    uint64_t value_id = ObjectId::VALUE_BOOL_MASK | 0x01;
    properties.push_back({edge_id, key_id, value_id});
}

void add_edge_prop_false() {
    property_count++;
    uint64_t value_id = ObjectId::VALUE_BOOL_MASK | 0x00;
    properties.push_back({edge_id, key_id, value_id});
}

void finish_wrong_line() {
    current_line++;
}

void finish_node_line() {
    current_line++;
    node_count++;
}

void finish_edge_line() {
    current_line++;
    edge_count++;
}

void print_error() {
    std::cout << "ERROR on line " << current_line << "\n";
}

int get_transition(int state, int token) {
    state_funcs[STATES*state + token]();
    return state_transitions[STATES*state + token];
}

void set_transition(int state, int token, int value, void (*func)()) {
    // auto a = &do_nothing;
    state_funcs[STATES*state + token] = func;
    state_transitions[STATES*state + token] = value;
}

int main() {
    auto start = std::chrono::system_clock::now();

    ExternalString::strings = external_strings;

    srand(1);
    state_transitions = new int[TOKENS*STATES];

    // llenar vacío
    for (int s = 0; s < STATES; s++) {
        for (int t = 1; t < TOKENS; t++) {
            set_transition(s, t, State::WRONG_LINE, &print_error);
        }
    }
    // ignore whitespace token
    for (int s = 0; s < STATES; s++) {
        set_transition(s, Token::WHITESPACE, s, &do_nothing);
    }

     // wrong line stays wrong (without giving more errors) until endline
    for (int t = 0; t < TOKENS; t++) {
        set_transition(State::WRONG_LINE, t, State::WRONG_LINE, &do_nothing);
    }
    set_transition(State::WRONG_LINE, Token::ENDLINE, State::LINE_BEGIN, &finish_wrong_line);

    set_transition(State::LINE_BEGIN, Token::IDENTIFIER, State::FIRST_ID, &save_first_id_identifier);
    set_transition(State::LINE_BEGIN, Token::ANON,       State::FIRST_ID, &save_first_id_anon);
    set_transition(State::LINE_BEGIN, Token::STRING,     State::FIRST_ID, &save_first_id_string);
    set_transition(State::LINE_BEGIN, Token::IRI,        State::FIRST_ID, &save_first_id_iri);
    set_transition(State::LINE_BEGIN, Token::INTEGER,    State::FIRST_ID, &save_first_id_int);
    set_transition(State::LINE_BEGIN, Token::FLOAT,      State::FIRST_ID, &save_first_id_float);
    set_transition(State::LINE_BEGIN, Token::TRUE,       State::FIRST_ID, &save_first_id_true);
    set_transition(State::LINE_BEGIN, Token::FALSE,      State::FIRST_ID, &save_first_id_false);
    set_transition(State::LINE_BEGIN, Token::IMPLICIT,   State::IMPLICIT_EDGE, &save_first_id_implicit);

    set_transition(State::FIRST_ID,     Token::COLON, State::EXPECT_NODE_LABEL, &do_nothing);
    set_transition(State::NODE_DEFINED, Token::COLON, State::EXPECT_NODE_LABEL, &do_nothing);

    set_transition(State::FIRST_ID,     Token::ENDLINE, State::LINE_BEGIN, &finish_node_line);
    set_transition(State::NODE_DEFINED, Token::ENDLINE, State::LINE_BEGIN, &finish_node_line);

    // TODO: accept IRI as label?
    set_transition(State::EXPECT_NODE_LABEL, Token::IDENTIFIER, State::NODE_DEFINED, &add_node_label);

    set_transition(State::FIRST_ID,     Token::IDENTIFIER, State::EXPECT_NODE_PROP_COLON, &save_prop_key);
    set_transition(State::NODE_DEFINED, Token::IDENTIFIER, State::EXPECT_NODE_PROP_COLON, &save_prop_key);

    set_transition(State::EXPECT_NODE_PROP_COLON, Token::COLON, State::EXPECT_NODE_PROP_VALUE, &do_nothing);

    // TODO: accept IRI as prop value
    set_transition(State::EXPECT_NODE_PROP_VALUE, Token::STRING,  State::NODE_DEFINED, &add_node_prop_string);
    set_transition(State::EXPECT_NODE_PROP_VALUE, Token::INTEGER, State::NODE_DEFINED, &add_node_prop_int);
    set_transition(State::EXPECT_NODE_PROP_VALUE, Token::FLOAT,   State::NODE_DEFINED, &add_node_prop_float);
    set_transition(State::EXPECT_NODE_PROP_VALUE, Token::FALSE,   State::NODE_DEFINED, &add_node_prop_false);
    set_transition(State::EXPECT_NODE_PROP_VALUE, Token::TRUE,    State::NODE_DEFINED, &add_node_prop_true);

    set_transition(State::IMPLICIT_EDGE, Token::L_ARROW, State::EXPECT_EDGE_SECOND, &set_left_direction);
    set_transition(State::IMPLICIT_EDGE, Token::R_ARROW, State::EXPECT_EDGE_SECOND, &set_right_direction);
    set_transition(State::FIRST_ID, Token::L_ARROW, State::EXPECT_EDGE_SECOND, &set_left_direction);
    set_transition(State::FIRST_ID, Token::R_ARROW, State::EXPECT_EDGE_SECOND, &set_right_direction);

    set_transition(State::EXPECT_EDGE_SECOND, Token::IDENTIFIER, State::EXPECT_EDGE_TYPE_COLON, &save_second_id_identifier);
    set_transition(State::EXPECT_EDGE_SECOND, Token::ANON,       State::EXPECT_EDGE_TYPE_COLON, &save_second_id_anon);
    set_transition(State::EXPECT_EDGE_SECOND, Token::STRING,     State::EXPECT_EDGE_TYPE_COLON, &save_second_id_string);
    set_transition(State::EXPECT_EDGE_SECOND, Token::IRI,        State::EXPECT_EDGE_TYPE_COLON, &save_second_id_iri);
    set_transition(State::EXPECT_EDGE_SECOND, Token::INTEGER,    State::EXPECT_EDGE_TYPE_COLON, &save_second_id_int);
    set_transition(State::EXPECT_EDGE_SECOND, Token::FLOAT,      State::EXPECT_EDGE_TYPE_COLON, &save_second_id_float);
    set_transition(State::EXPECT_EDGE_SECOND, Token::TRUE,       State::EXPECT_EDGE_TYPE_COLON, &save_second_id_true);
    set_transition(State::EXPECT_EDGE_SECOND, Token::FALSE,      State::EXPECT_EDGE_TYPE_COLON, &save_second_id_false);

    set_transition(State::EXPECT_EDGE_TYPE_COLON, Token::COLON, State::EXPECT_EDGE_TYPE, &do_nothing);

    // TODO: accept IRI as type?
    set_transition(State::EXPECT_EDGE_TYPE, Token::IDENTIFIER, State::EDGE_DEFINED, &save_edge_type);

    set_transition(State::EDGE_DEFINED, Token::ENDLINE,    State::LINE_BEGIN, &finish_edge_line);
    set_transition(State::EDGE_DEFINED, Token::IDENTIFIER, State::EXPECT_EDGE_PROP_COLON, &save_prop_key);

    set_transition(State::EXPECT_EDGE_PROP_COLON, Token::COLON, State::EXPECT_EDGE_PROP_VALUE, &do_nothing);

    // TODO: accept IRI as prop value
    set_transition(State::EXPECT_EDGE_PROP_VALUE, Token::STRING,  State::EDGE_DEFINED, &add_edge_prop_string);
    set_transition(State::EXPECT_EDGE_PROP_VALUE, Token::INTEGER, State::EDGE_DEFINED, &add_edge_prop_int);
    set_transition(State::EXPECT_EDGE_PROP_VALUE, Token::FLOAT,   State::EDGE_DEFINED, &add_edge_prop_float);
    set_transition(State::EXPECT_EDGE_PROP_VALUE, Token::FALSE,   State::EDGE_DEFINED, &add_edge_prop_false);
    set_transition(State::EXPECT_EDGE_PROP_VALUE, Token::TRUE,    State::EDGE_DEFINED, &add_edge_prop_true);

    // lexer.begin();
    int current_state = State::LINE_BEGIN;
    while (int token = lexer.next_token()) {
        current_state = get_transition(current_state, token);
    }

    std::cout << "node count: " << node_count << "\n";
    std::cout << "label count: " << labels.size() << "\n";
    std::cout << "property count: " << properties.size() << "\n";
    std::cout << "edge count: " << edges.size() << "\n";

    auto end_lexer = std::chrono::system_clock::now();
    std::chrono::duration<float, std::milli> parser_duration = end_lexer - start;
    std::cout << "Parser duration: " << parser_duration.count() << " ms\n";

    std::fstream object_file;
    object_file.open("db/object_file.dat", std::ios::out|std::ios::binary);
    object_file.write(external_strings, external_strings_end);

    {
        std::sort(edges.begin(), edges.end());
        auto* i = edges.data();
        auto* end = edges.end().operator->();
        BPTLeafWriter<4> leaf_writer("db/from_to_type_edge.dat");
        BPTDirWriter<4> dir_writer("db/from_to_type_edge.dir");
        uint32_t current_block = 0;
        while (i < end) {
            char* begin = (char*) i;

            // skip first leaf from going into bulk_import
            if (i != edges.data()) {
                dir_writer.bulk_insert(i, 0, current_block);
            }
            i += leaf_writer.max_records;
            if (i < end) {
                leaf_writer.process_block(begin, leaf_writer.max_records, ++current_block);
            } else {
                leaf_writer.process_block(begin, edges.size() % leaf_writer.max_records, 0);
            }
        }
    }

    {
        for (auto& edge : edges) {
            // change permutation
            auto aux = edge[0];
            edge[0] = edge[1];
            edge[1] = edge[2];
            edge[2] = aux;
        }
        std::sort(edges.begin(), edges.end());
        auto* i = edges.data();
        auto* end = edges.end().operator->();
        BPTLeafWriter<4> leaf_writer("db/to_type_from_edge.dat");
        BPTDirWriter<4> dir_writer("db/to_type_from_edge.dir");
        uint32_t current_block = 0;
        while (i < end) {
            char* begin = (char*) i;

            // skip first leaf from going into bulk_import
            if (i != edges.data()) {
                dir_writer.bulk_insert(i, 0, current_block);
            }
            i += leaf_writer.max_records;
            if (i < end) {
                leaf_writer.process_block(begin, leaf_writer.max_records, ++current_block);
            } else {
                leaf_writer.process_block(begin, edges.size() % leaf_writer.max_records, 0);
            }
        }
    }

    {
        for (auto& edge : edges) {
            // change permutation
            auto aux = edge[0];
            edge[0] = edge[1];
            edge[1] = edge[2];
            edge[2] = aux;
        }
        std::sort(edges.begin(), edges.end());
        auto* i = edges.data();
        auto* end = edges.end().operator->();
        BPTLeafWriter<4> leaf_writer("db/type_from_to_edge.dat");
        BPTDirWriter<4> dir_writer("db/type_from_to_edge.dir");
        uint32_t current_block = 0;
        while (i < end) {
            char* begin = (char*) i;

            // skip first leaf from going into bulk_import
            if (i != edges.data()) {
                dir_writer.bulk_insert(i, 0, current_block);
            }
            i += leaf_writer.max_records;
            if (i < end) {
                leaf_writer.process_block(begin, leaf_writer.max_records, ++current_block);
            } else {
                leaf_writer.process_block(begin, edges.size() % leaf_writer.max_records, 0);
            }
        }
    }

    // int limit = 10, count = 0;
    // for (auto& edge : edges) {
    //     if (count++ == limit) break;
    //     std::cout << decode_id(edge[0]) << ", " << decode_id(edge[1]) << ", " << decode_id(edge[2]) << ", " << edge[3] << '\n';
    //     // std::cout << edge[0] << ", " << edge[1] << ", " << edge[2]<< ", " << edge[3] << '\n';
    // }

    // std::sort(properties.begin(), properties.end());
    // std::sort(labels.begin(), labels.end());

    auto end_order = std::chrono::system_clock::now();
    std::chrono::duration<float, std::milli> order_duration = end_order - end_lexer;
    std::cout << "Order duration: " << order_duration.count() << " ms\n";

    // for (uint64_t i = 1; i < external_strings_end; i++) {
    //     std::cout << external_strings[i];
    // }
}

