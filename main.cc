// #define XXH_STATIC_LINKING_ONLY   /* access advanced declarations */
// #define XXH_IMPLEMENTATION   /* access definitions */
#define XXH_INLINE_ALL  // TODO: poner en xxhash/xxhash.h ?
#define XXH_VECTOR XXH_AVX2

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <vector>

#include "catalog/quad_catalog.h"
#include "bpt_leaf_writer.h"
#include "bpt_dir_writer.h"
#include "edge_table_writer.h"
#include "inliner.h"
#include "lexer.h"
#include "object_id.h"
#include "robin_hood.h"
#include "state.h"
#include "xxhash/xxhash.h"

// Estados:
int* state_transitions;
void (*state_funcs [Token::TOTAL_TOKENS * State::TOTAL_STATES])();
Lexer lexer;
int current_line = 1;

uint64_t id1;
uint64_t id2;
uint64_t edge_id;
uint64_t edge_count = 0;
std::vector<uint64_t> ids_stack;

uint64_t type_id;
uint64_t key_id;

// true if right
bool direction;

QuadCatalog catalog("db/catalog.dat");

std::vector<std::array<uint64_t, 1>> declared_nodes;
std::vector<std::array<uint64_t, 2>> labels;
std::vector<std::array<uint64_t, 3>> properties;
std::vector<std::array<uint64_t, 4>> edges;
std::vector<std::array<uint64_t, 3>> equal_from_to;
std::vector<std::array<uint64_t, 3>> equal_from_type;
std::vector<std::array<uint64_t, 3>> equal_to_type;
std::vector<std::array<uint64_t, 2>> equal_from_to_type;

size_t external_strings_initial_size = 1024ULL * 1024ULL * 1024ULL * 10ULL;
char* external_strings = new char[external_strings_initial_size];

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
}

// const char* decode_id(uint64_t id) {
//     auto type_mask = id & ObjectId::TYPE_MASK;
//     auto value_mask = id & ObjectId::VALUE_MASK;
//     if (type_mask == ObjectId::IDENTIFIABLE_EXTERNAL_MASK) {
//         return &external_strings[value_mask];
//     } else {
//         char* c = new char[8];
//         int shift_size = 6*8;
//         for (int i = 0; i < ObjectId::MAX_INLINED_BYTES; i++) {
//             uint8_t byte = (id >> shift_size) & 0xFF;
//             c[i] = byte;
//             shift_size -= 8;
//         }
//         c[7] = '\0';
//         return c;
//     }
// }

void do_nothing() { }
void set_left_direction() { direction = false; }
void set_right_direction() { direction = true; }

void save_first_id_identifier() {
    ids_stack.clear();
    if (lexer.str_len < 8) {
        id1 = Inliner::inline_string(lexer.str) | ObjectId::IDENTIFIABLE_INLINED_MASK ;
    } else {
        id1 = get_or_create_external_string_id() | ObjectId::IDENTIFIABLE_EXTERNAL_MASK;
    }
}

void save_first_id_anon() {
    ids_stack.clear();
    uint64_t unmasked_id = std::stoull(lexer.str + 2);
    id1 = unmasked_id | ObjectId::ANONYMOUS_NODE_MASK;
    if (unmasked_id > catalog.anonymous_nodes_count) {
        catalog.anonymous_nodes_count = unmasked_id;
    }
}

void save_first_id_string() {
    ids_stack.clear();
    normalize_string_literal();

    if (lexer.str_len < 8) {
        id1 = Inliner::inline_string(lexer.str) | ObjectId::VALUE_INLINE_STR_MASK ;
    } else {
        id1 = get_or_create_external_string_id() | ObjectId::VALUE_EXTERNAL_STR_MASK;
    }
}

void save_first_id_iri() {
    ids_stack.clear();
    //TODO:
}

void save_first_id_int() {
    ids_stack.clear();
    id1 = Inliner::inline_int(atoll(lexer.str));
}

void save_first_id_float() {
    ids_stack.clear();
    id1 = Inliner::inline_float(atof(lexer.str));
}

void save_first_id_true() {
    ids_stack.clear();
    id1 = ObjectId::VALUE_BOOL_MASK | 0x01;
}

void save_first_id_false() {
    ids_stack.clear();
    id1 = ObjectId::VALUE_BOOL_MASK | 0x00;
}

void save_first_id_implicit() {
    // TODO:
    if (ids_stack.size() == 0) {
        // throw ImportException("[line " + std::to_string(line_number)
            // + "] can't use implicit edge on undefined object");
    }
    else if (lexer.str_len < ids_stack.size()) {
        id1 = ids_stack[lexer.str_len-1];
        ids_stack.resize(lexer.str_len);
        ids_stack.push_back(edge_id);
    }
    else if (lexer.str_len == ids_stack.size()) {
        id1 = ids_stack[lexer.str_len-1];
        ids_stack.push_back(edge_id);
    }
    else {
        // throw ImportException("[line " + std::to_string(line_number)
        //     + "] undefined level of implicit edge");
    }
}

void save_edge_type() {
    if (lexer.str_len < 8) {
        type_id = Inliner::inline_string(lexer.str) | ObjectId::IDENTIFIABLE_INLINED_MASK ;
    } else {
        type_id = get_or_create_external_string_id() | ObjectId::IDENTIFIABLE_EXTERNAL_MASK;
    }
    ++catalog.type2total_count[type_id];

    edge_id = ++edge_count | ObjectId::CONNECTION_MASK;
    if (direction) {
        edges.push_back({id1, id2, type_id, edge_id});
    } else {
        edges.push_back({id2, id1, type_id, edge_id});
    }

    if (id1 == id2) {
        equal_from_to.push_back({id1, type_id, edge_id});

        if (id1 == type_id) {
            equal_from_to_type.push_back({id1, edge_id});
        }
    }
    if (id1 == type_id) {
        if (direction) {
            equal_from_type.push_back({id1, id2, edge_id});
        } else {
            equal_to_type.push_back({id1, id2, edge_id});
        }
    }
    if (id2 == type_id) {
        if (direction) {
            equal_to_type.push_back({id1, id2, edge_id});
        } else {
            equal_from_type.push_back({id1, id2, edge_id});
        }
    }

    ids_stack.push_back(edge_id);
}

void save_prop_key() {
    if (lexer.str_len < 8) {
        key_id = Inliner::inline_string(lexer.str) | ObjectId::VALUE_INLINE_STR_MASK ;
    } else {
        key_id = get_or_create_external_string_id() | ObjectId::VALUE_EXTERNAL_STR_MASK  ;
    }
}

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
    if (unmasked_id > catalog.anonymous_nodes_count) {
        catalog.anonymous_nodes_count = unmasked_id;
    }
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
    uint64_t label_id;
    if (lexer.str_len < 8) {
        label_id = Inliner::inline_string(lexer.str) | ObjectId::VALUE_INLINE_STR_MASK ;
    } else {
        label_id = get_or_create_external_string_id() | ObjectId::VALUE_EXTERNAL_STR_MASK;
    }
    labels.push_back({id1, label_id});
    ++catalog.label2total_count[label_id];
}

void add_node_prop_string() {
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
    uint64_t value_id = Inliner::inline_int(atoll(lexer.str));
    properties.push_back({id1, key_id, value_id});
}

void add_node_prop_float() {
    uint64_t value_id = Inliner::inline_float(atof(lexer.str));
    properties.push_back({id1, key_id, value_id});
}

void add_node_prop_true() {
    uint64_t value_id = ObjectId::VALUE_BOOL_MASK | 0x01;
    properties.push_back({id1, key_id, value_id});
}

void add_node_prop_false() {
    uint64_t value_id = ObjectId::VALUE_BOOL_MASK | 0x00;
    properties.push_back({id1, key_id, value_id});
}

void add_edge_prop_string() {
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
    uint64_t value_id = Inliner::inline_int(atoll(lexer.str));
    properties.push_back({edge_id, key_id, value_id});
}

void add_edge_prop_float() {
    uint64_t value_id = Inliner::inline_float(atof(lexer.str));
    properties.push_back({edge_id, key_id, value_id});
}

void add_edge_prop_true() {
    uint64_t value_id = ObjectId::VALUE_BOOL_MASK | 0x01;
    properties.push_back({edge_id, key_id, value_id});
}

void add_edge_prop_false() {
    uint64_t value_id = ObjectId::VALUE_BOOL_MASK | 0x00;
    properties.push_back({edge_id, key_id, value_id});
}

void finish_wrong_line() {
    current_line++;
}

void finish_node_line() {
    declared_nodes.push_back({id1});
    ids_stack.push_back(id1);
    current_line++;
}

void finish_edge_line() {
    current_line++;
}

void print_error() {
    std::cout << "ERROR on line " << current_line << "\n";
}

int get_transition(int state, int token) {
    state_funcs[State::TOTAL_STATES*state + token]();
    return state_transitions[State::TOTAL_STATES*state + token];
}

void set_transition(int state, int token, int value, void (*func)()) {
    state_funcs[State::TOTAL_STATES*state + token] = func;
    state_transitions[State::TOTAL_STATES*state + token] = value;
}

template <std::size_t N>
void reorder_cols(std::vector<std::array<uint64_t, N>>& values,
                  std::array<size_t, N>& current_permutation,
                  std::array<size_t, N>&& new_permutation)
{
    static_assert(N == 2 || N == 3 || N == 4, "Unsuported N");
    assert(current_permutation.size() == new_permutation.size());

    if (N == 2) {
        if (current_permutation[0] == new_permutation[0]) {
            assert(current_permutation[1] == new_permutation[1]);
        }
        else {
            for (auto& value : values) {
                std::swap(value[0], value[1]);
            }
        }
    }

    if (N == 3) {
        if (current_permutation[0] == new_permutation[0]
         && current_permutation[1] == new_permutation[1]
         && current_permutation[2] == new_permutation[2])
        {
            // do nothing
        }
        else if (new_permutation[0] == current_permutation[1]
              && new_permutation[1] == current_permutation[2]
              && new_permutation[2] == current_permutation[0])
        {
            for (auto& value : values) {
                auto aux  = value[0];
                value[0] = value[1];
                value[1] = value[2];
                value[2] = aux;
            }
        }
        else if (new_permutation[0] == current_permutation[1]
              && new_permutation[1] == current_permutation[0]
              && new_permutation[2] == current_permutation[2])
        {
            for (auto& value : values) {
                auto aux  = value[0];
                value[0] = value[1];
                value[1] = aux;
            }
        }
        else {
            throw std::invalid_argument("Unsuported permutation");
        }
    }

    if (N == 4) {
        if (current_permutation[0] == new_permutation[0]
         && current_permutation[1] == new_permutation[1]
         && current_permutation[2] == new_permutation[2]
         && current_permutation[3] == new_permutation[3])
        {
            // do nothing
        }
        else if (new_permutation[0] == current_permutation[1]
              && new_permutation[1] == current_permutation[2]
              && new_permutation[2] == current_permutation[0]
              && new_permutation[3] == current_permutation[3])
        {
            for (auto& value : values) {
                auto aux  = value[0];
                value[0] = value[1];
                value[1] = value[2];
                value[2] = aux;
            }
        }
        else if (new_permutation[0] == current_permutation[0]
              && new_permutation[1] == current_permutation[2]
              && new_permutation[2] == current_permutation[1]
              && new_permutation[3] == current_permutation[3])
        {
            for (auto& value : values) {
                auto aux  = value[1];
                value[1] = value[2];
                value[2] = aux;
            }
        }
        else {
            throw std::invalid_argument("Unsuported permutation");
        }
    }
    current_permutation = std::move(new_permutation);
}


template <std::size_t N>
void create_bpt(const std::string& base_name, std::vector<std::array<uint64_t, N>>& values) {
    std::sort(values.begin(), values.end());

    BPTLeafWriter<N> leaf_writer(base_name + ".leaf");
    BPTDirWriter<N> dir_writer(base_name + ".dir");

    auto* i = values.data();
    auto* end = values.end().operator->();
    uint32_t current_block = 0;

    if (values.size() == 0) {
        leaf_writer.make_empty();
    }

    while (i < end) {
        char* begin = (char*) i;

        // skip first leaf from going into bulk_import
        if (i != values.data()) {
            dir_writer.bulk_insert(i, 0, current_block);
        }
        i += leaf_writer.max_records;
        if (i < end) {
            leaf_writer.process_block(begin, leaf_writer.max_records, ++current_block);
        } else {
            leaf_writer.process_block(begin, values.size() % leaf_writer.max_records, 0);
        }
    }
}

void create_table(const std::string& base_name) {
    EdgeTableWriter table_writer(base_name + ".table");

    for (const auto& edge : edges) {
        table_writer.insert_tuple(edge);
    }
}

int main() {
    auto start = std::chrono::system_clock::now();

    ExternalString::strings = external_strings;
    catalog.anonymous_nodes_count = 0;

    state_transitions = new int[Token::TOTAL_TOKENS*State::TOTAL_STATES];

    // llenar vacío
    for (int s = 0; s < State::TOTAL_STATES; s++) {
        for (int t = 1; t < Token::TOTAL_TOKENS; t++) {
            set_transition(s, t, State::WRONG_LINE, &print_error);
        }
    }
    // ignore whitespace token
    for (int s = 0; s < State::TOTAL_STATES; s++) {
        set_transition(s, Token::WHITESPACE, s, &do_nothing);
    }

     // wrong line stays wrong (without giving more errors) until endline
    for (int t = 0; t < Token::TOTAL_TOKENS; t++) {
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

    // TODO: accept IRI as prop value?
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

    // TODO: accept IRI as prop value?
    set_transition(State::EXPECT_EDGE_PROP_VALUE, Token::STRING,  State::EDGE_DEFINED, &add_edge_prop_string);
    set_transition(State::EXPECT_EDGE_PROP_VALUE, Token::INTEGER, State::EDGE_DEFINED, &add_edge_prop_int);
    set_transition(State::EXPECT_EDGE_PROP_VALUE, Token::FLOAT,   State::EDGE_DEFINED, &add_edge_prop_float);
    set_transition(State::EXPECT_EDGE_PROP_VALUE, Token::FALSE,   State::EDGE_DEFINED, &add_edge_prop_false);
    set_transition(State::EXPECT_EDGE_PROP_VALUE, Token::TRUE,    State::EDGE_DEFINED, &add_edge_prop_true);

    // TODO: set stdin or file according to input
    // lexer.begin();
    int current_state = State::LINE_BEGIN;
    while (int token = lexer.next_token()) {
        current_state = get_transition(current_state, token);
    }

    auto end_lexer = std::chrono::system_clock::now();
    std::chrono::duration<float, std::milli> parser_duration = end_lexer - start;
    std::cout << "Parser duration: " << parser_duration.count() << " ms\n";

    {
        // WRITE OBJECT FILE
        std::fstream object_file;
        object_file.open("db/object_file.dat", std::ios::out|std::ios::binary);
        object_file.write(external_strings, external_strings_end);
        delete[] external_strings;
    }

    {   // NODES
        // write missing nodes from edges
        robin_hood::unordered_set<uint64_t> declared_nodes_set;

        for (auto n : declared_nodes) {
            declared_nodes_set.insert(n[0]);
        }

        for (const auto& edge : edges) {
            if (declared_nodes_set.insert(edge[0]).second) {
                declared_nodes.push_back({edge[0]});
            }
            if (declared_nodes_set.insert(edge[1]).second) {
                declared_nodes.push_back({edge[1]});
            }
        }
        create_bpt("db/nodes", declared_nodes);
    }

    {
        size_t COL_NODE = 0, COL_LABEL = 1;
        std::array<size_t, 2> current_label = { COL_NODE, COL_LABEL };

        reorder_cols(labels, current_label, { COL_NODE, COL_LABEL });
        create_bpt("db/node_label", labels);

        reorder_cols(labels, current_label, { COL_LABEL, COL_NODE });
        create_bpt("db/label_node", labels);
    }

    {
        size_t COL_OBJ = 0, COL_KEY = 1, COL_VALUE = 2;
        std::array<size_t, 3> current_prop = { COL_OBJ, COL_KEY, COL_VALUE };

        reorder_cols(properties, current_prop, { COL_OBJ, COL_KEY, COL_VALUE });
        create_bpt("db/obj_key_value", properties);

        reorder_cols(properties, current_prop, { COL_KEY, COL_VALUE, COL_OBJ });
        create_bpt("db/key_value_obj", properties);
    }

    // 
    {
        // TODO: use robin hood unordered map
        std::map<uint64_t, uint64_t> map_key_count;
        std::map<uint64_t, uint64_t> map_distinct_values;
        uint64_t current_key     = 0;
        uint64_t current_value   = 0;
        uint64_t key_count       = 0;
        uint64_t distinct_values = 0;

        // properties_ordered_file.begin_read();
        // auto record = properties_ordered_file.next_record();
        for (const auto& record : properties) {
            // check same key
            if (record[0] == current_key) {
                ++key_count;
                // check if value changed
                if (record[1] != current_value) {
                    ++distinct_values;
                    current_value = record[1];
                }
            } else {
                // save stats from last key
                if (current_key != 0) {
                    map_key_count.insert({ current_key, key_count });
                    map_distinct_values.insert({ current_key, distinct_values });
                }
                current_key   = record[0];
                current_value = record[1];

                key_count       = 1;
                distinct_values = 1;
            }
        }
        // save stats from last key
        if (current_key != 0) {
            map_key_count.insert({ current_key, key_count });
            map_distinct_values.insert({ current_key, distinct_values });
        }

        catalog.key2distinct    = move(map_distinct_values);
        catalog.key2total_count = move(map_key_count);
        catalog.distinct_keys   = catalog.key2total_count.size();
    }

    // Must write edge table before ordering
    create_table("db/edges");

    {
        size_t COL_FROM = 0, COL_TO = 1, COL_TYPE = 2, COL_EDGE = 3;
        std::array<size_t, 4> current_edge = { COL_FROM, COL_TO, COL_TYPE, COL_EDGE };

        reorder_cols(edges, current_edge, { COL_FROM, COL_TO, COL_TYPE, COL_EDGE });
        create_bpt("db/from_to_type_edge", edges);

        { // set catalog.distinct_from
            uint64_t distinct_from = 0;
            uint64_t current_from  = 0;

            for (const auto& record : edges) {
                if (record[0] != current_from) {
                    ++distinct_from;
                    current_from = record[0];
                }
            }
            catalog.distinct_from = distinct_from;
        }

        reorder_cols(edges, current_edge, { COL_TO, COL_TYPE, COL_FROM, COL_EDGE });
        create_bpt("db/to_type_from_edge", edges);

        { // set catalog.distinct_to
            uint64_t distinct_to = 0;
            uint64_t current_from  = 0;

            for (const auto& record : edges) {
                if (record[0] != current_from) {
                    ++distinct_to;
                    current_from = record[0];
                }
            }
            catalog.distinct_to = distinct_to;
        }

        reorder_cols(edges, current_edge, { COL_TYPE, COL_FROM, COL_TO, COL_EDGE });
        create_bpt("db/type_from_to_edge", edges);

        reorder_cols(edges, current_edge, { COL_TYPE, COL_TO, COL_FROM, COL_EDGE });
        create_bpt("db/type_to_from_edge", edges);
    }

    {   // FROM=TO=TYPE EDGE
        size_t COL_FROM_TO_TYPE = 0, COL_EDGE = 1;
        std::array<size_t, 2> current_edge = { COL_FROM_TO_TYPE, COL_EDGE };

        reorder_cols(equal_from_to_type, current_edge, { COL_FROM_TO_TYPE, COL_EDGE });
        create_bpt("db/equal_from_to_type", equal_from_to_type);

        {
            // create catalog.type2equal_from_to_type_count
            uint64_t current_type = 0;
            uint64_t count        = 0;
            for (const auto& record : equal_from_to_type) {
                // check same key
                if (record[0] == current_type) {
                    ++count;
                } else {
                    // save stats from last key
                    if (current_type != 0) {
                        catalog.type2equal_from_to_type_count.insert({ current_type, count });
                    }
                    current_type = record[0];
                    count = 1;
                }
            }
            // save stats from last key
            if (current_type != 0) {
                catalog.type2equal_from_to_type_count.insert({ current_type, count });
            }
        }
    }

    {   // FROM=TO TYPE EDGE
        size_t COL_FROM_TO = 0, COL_TYPE = 1, COL_EDGE = 2;
        std::array<size_t, 3> current_edge = { COL_FROM_TO, COL_TYPE, COL_EDGE };

        reorder_cols(equal_from_to, current_edge, { COL_FROM_TO, COL_TYPE, COL_EDGE });
        create_bpt("db/equal_from_to", equal_from_to);

        reorder_cols(equal_from_to, current_edge, { COL_TYPE, COL_FROM_TO, COL_EDGE });
        create_bpt("db/equal_from_to_inverted", equal_from_to);

        {
            // create catalog.type2equal_from_to_count
            // calling this AFTER inverted, so type is at pos 0
            uint64_t current_type = 0;
            uint64_t count        = 0;
            for (const auto& record : equal_from_to) {
                // check same key
                if (record[0] == current_type) {
                    ++count;
                } else {
                    // save stats from last key
                    if (current_type != 0) {
                        catalog.type2equal_from_to_count.insert({ current_type, count });
                    }
                    current_type = record[0];
                    count = 1;
                }
            }
            // save stats from last key
            if (current_type != 0) {
                catalog.type2equal_from_to_count.insert({ current_type, count });
            }
        }
    }

    {   // FROM=TYPE TO EDGE
        size_t COL_FROM_TYPE = 0, COL_TO = 1, COL_EDGE = 2;
        std::array<size_t, 3> current_edge = { COL_FROM_TYPE, COL_TO, COL_EDGE };

        reorder_cols(equal_from_type, current_edge, { COL_FROM_TYPE, COL_TO, COL_EDGE });
        create_bpt("db/equal_from_type", equal_from_type);

        {
            // create catalog.type2equal_from_type_count
            // calling this BEFORE inverted, so type is at pos 0
            uint64_t current_type = 0;
            uint64_t count        = 0;
            for (const auto& record : equal_from_type) {
                // check same key
                if (record[0] == current_type) {
                    ++count;
                } else {
                    // save stats from last key
                    if (current_type != 0) {
                        catalog.type2equal_from_type_count.insert({ current_type, count });
                    }
                    current_type = record[0];
                    count = 1;
                }
            }
            // save stats from last key
            if (current_type != 0) {
                catalog.type2equal_from_type_count.insert({ current_type, count });
            }
        }

        reorder_cols(equal_from_type, current_edge, { COL_TO, COL_FROM_TYPE, COL_EDGE });
        create_bpt("db/equal_from_type_inverted", equal_from_type);
    }

    {   // TO=TYPE FROM EDGE
        size_t COL_TO_TYPE = 0, COL_FROM = 1, COL_EDGE = 2;
        std::array<size_t, 3> current_edge = { COL_TO_TYPE, COL_FROM, COL_EDGE };

        reorder_cols(equal_to_type, current_edge, { COL_TO_TYPE, COL_FROM, COL_EDGE });
        create_bpt("db/equal_to_type", equal_to_type);

        {
            // create catalog.type2equal_to_type_count
            // calling this BEFORE inverted, so type is at pos 0
            uint64_t current_type = 0;
            uint64_t count        = 0;
            for (const auto& record : equal_from_to) {
                // check same key
                if (record[0] == current_type) {
                    ++count;
                } else {
                    // save stats from last key
                    if (current_type != 0) {
                        catalog.type2equal_to_type_count.insert({ current_type, count });
                    }
                    current_type = record[0];
                    count = 1;
                }
            }
            // save stats from last key
            if (current_type != 0) {
                catalog.type2equal_to_type_count.insert({ current_type, count });
            }
        }

        reorder_cols(equal_to_type, current_edge, { COL_FROM, COL_TO_TYPE, COL_EDGE });
        create_bpt("db/equal_to_type_inverted", equal_to_type);
    }

    auto end_order = std::chrono::system_clock::now();
    std::chrono::duration<float, std::milli> order_duration = end_order - end_lexer;
    std::cout << "Order duration: " << order_duration.count() << " ms\n";

    // TODO:
    // catalog.identifiable_nodes_count // TODO: se deberia renombrar a named_nodes count
    // - puedo hacer merge entre bpt nodes y from_to_type_edge (liminando duplicados)

    {
        robin_hood::unordered_set<uint64_t> identifiable_nodes;
        for (const auto& edge : edges) {
            for (size_t i = 0; i < 3; i++) {
                if ((edge[i] & ObjectId::TYPE_MASK) == ObjectId::IDENTIFIABLE_EXTERNAL_MASK
                    || (edge[i] & ObjectId::TYPE_MASK) == ObjectId::IDENTIFIABLE_INLINED_MASK)
                {
                    identifiable_nodes.insert(edge[i]);
                }
            }
        }
        for (auto n : declared_nodes) {
            if ((n[0] & ObjectId::TYPE_MASK) == ObjectId::IDENTIFIABLE_EXTERNAL_MASK
                || (n[0] & ObjectId::TYPE_MASK) == ObjectId::IDENTIFIABLE_INLINED_MASK)
            {
                identifiable_nodes.insert(n[0]);
            }
        }
        catalog.identifiable_nodes_count = identifiable_nodes.size();
    }

    catalog.distinct_type = catalog.type2total_count.size(); // TODO: may be redundant
    catalog.distinct_labels = catalog.label2total_count.size(); // TODO: may be redundant
    catalog.connections_count = edges.size();
    catalog.label_count = labels.size();
    catalog.properties_count = properties.size();

    catalog.equal_from_to_count = equal_from_to.size();
    catalog.equal_from_type_count = equal_from_type.size();
    catalog.equal_to_type_count = equal_to_type.size();
    catalog.equal_from_to_type_count = equal_from_to_type.size();

    catalog.print();
    catalog.save_changes();
}

