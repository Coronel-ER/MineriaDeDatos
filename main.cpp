#include <iostream>     // cout, cerr
#include <fstream>      // ifstream
#include <sstream>      // stringstream
#include <string>       // string
#include <vector>       // vector
#include <unordered_map>// unordered_map
#include <thread>       // thread
#include <mutex>        // mutex
#include <condition_variable> // condition_variable
#include <atomic>       // atomic
#include <algorithm>    // transform
#include <cctype>       // isalnum, tolower
#include <unistd.h>     // pipe, write, close
#include <cstdio>       // fdopen, fgets
 
 
/* ─── constantes ──────────────────────────────────────────────── */
 
constexpr int NUM_WORKERS      = 3;
constexpr int BUFFER_CAPACITY  = 8;
constexpr int NUM_CHUNKS       = 4;
 
 
/* ─── ChunkDescriptor ─────────────────────────────────────────── */
 
struct ChunkDescriptor {
    int         chunk_id     = 0;
    std::string data;              // texto del chunk
    size_t      start_offset = 0;
    size_t      end_offset   = 0;
    bool        processed    = false;
};
 
 
/* ─── ChunkTable ──────────────────────────────────────────────── */
 
struct ChunkTable {
    std::vector<ChunkDescriptor> chunks;
};
 
 
/* ─── BoundedBuffer ───────────────────────────────────────────── */
 
class BoundedBuffer {
public:
    explicit BoundedBuffer(int capacity)
        : capacity_(capacity), items_(capacity, 0) {}
 
    /* productor: bloquea si está lleno */
    void push(int chunk_index) {
        std::unique_lock<std::mutex> lock(mutex_);
        not_full_.wait(lock, [this]{ return count_ < capacity_; });
 
        items_[tail_] = chunk_index;
        tail_ = (tail_ + 1) % capacity_;
        ++count_;
 
        not_empty_.notify_one();
    }
 
    /* consumidor: bloquea si está vacío */
    int pop() {
        std::unique_lock<std::mutex> lock(mutex_);
        not_empty_.wait(lock, [this]{ return count_ > 0; });
 
        int val = items_[head_];
        head_ = (head_ + 1) % capacity_;
        --count_;
 
        not_full_.notify_one();
        return val;
    }
 
private:
    int                     capacity_;
    std::vector<int>        items_;
    int                     head_  = 0;
    int                     tail_  = 0;
    int                     count_ = 0;
    std::mutex              mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;
};
 
 
/* ─── WordCountTable (wrapper sobre unordered_map) ────────────── */
 
using WordCountMap = std::unordered_map<std::string, int>;
 
 
/* ─── helpers de I/O ──────────────────────────────────────────── */
 
/* leer archivo completo a std::string */
std::string read_file(const std::string &filename) {
    std::ifstream file(filename, std::ios::binary);
    if (!file)
        throw std::runtime_error("No se pudo abrir: " + filename);
 
    return { std::istreambuf_iterator<char>(file),
             std::istreambuf_iterator<char>() };
}
 
/* ¿es carácter de palabra? */
inline bool is_word_char(char c) {
    return static_cast<bool>(std::isalnum(static_cast<unsigned char>(c)));
}
 
 
/* ─── construcción de chunks ──────────────────────────────────── */
 
ChunkTable build_chunks(const std::string &file_data, int num_chunks) {
    ChunkTable table;
    size_t file_size        = file_data.size();
    size_t approx_chunk_sz  = file_size / static_cast<size_t>(num_chunks);
    size_t start            = 0;
 
    for (int i = 0; i < num_chunks && start < file_size; ++i) {
        size_t end;
 
        if (i == num_chunks - 1) {
            end = file_size;
        } else {
            end = std::min(start + approx_chunk_sz, file_size);
            /* no cortar palabras a la mitad */
            while (end < file_size && is_word_char(file_data[end]))
                ++end;
        }
 
        ChunkDescriptor cd;
        cd.chunk_id     = static_cast<int>(table.chunks.size());
        cd.data         = file_data.substr(start, end - start);
        cd.start_offset = start;
        cd.end_offset   = end;
 
        table.chunks.push_back(std::move(cd));
        start = end;
    }
 
    return table;
}
 
 
/* ─── procesamiento de texto ──────────────────────────────────── */
 
WordCountMap process_chunk_text(const std::string &data) {
    WordCountMap local;
    std::string  word;
 
    for (unsigned char ch : data) {
        if (std::isalnum(ch)) {
            word += static_cast<char>(std::tolower(ch));
        } else if (!word.empty()) {
            ++local[word];
            word.clear();
        }
    }
 
    if (!word.empty())
        ++local[word];
 
    return local;
}
 
 
/* ─── pipes: clasificación y envío ───────────────────────────── */
 
/* 0 = a..m | 1 = n..z | -1 = inválida */
int classify_word_pipe(const std::string &word) {
    if (word.empty()) return -1;
    char c = static_cast<char>(std::tolower(static_cast<unsigned char>(word[0])));
    if (c >= 'a' && c <= 'm') return 0;
    if (c >= 'n' && c <= 'z') return 1;
    return -1;
}
 
/* escritura completa al fd */
bool write_all(int fd, const char *buf, size_t len) {
    size_t total = 0;
    while (total < len) {
        ssize_t n = write(fd, buf + total, len - total);
        if (n <= 0) return false;
        total += static_cast<size_t>(n);
    }
    return true;
}
 
/* enviar "word|count\n" al pipe */
bool send_to_pipe(int fd, const std::string &word, int count) {
    std::string line = word + '|' + std::to_string(count) + '\n';
    return write_all(fd, line.c_str(), line.size());
}
 
/* vaciar tabla local hacia los dos pipes */
bool flush_to_pipes(const WordCountMap &table, int fd_first, int fd_second) {
    for (auto &[word, count] : table) {
        int bucket = classify_word_pipe(word);
        if (bucket == 0 && !send_to_pipe(fd_first,  word, count)) return false;
        if (bucket == 1 && !send_to_pipe(fd_second, word, count)) return false;
    }
    return true;
}
 
 
/* ─── WorkerArgs ──────────────────────────────────────────────── */
 
struct WorkerArgs {
    int          worker_id;
    ChunkTable  *chunk_table;
    BoundedBuffer *buffer;
    int          pipe_first_half[2];
    int          pipe_second_half[2];
};
 
 
/* ─── hilo trabajador ─────────────────────────────────────────── */
 
void worker_thread(WorkerArgs args) {
    while (true) {
        int chunk_index = args.buffer->pop();
        if (chunk_index == -1) break;   // centinela
 
        ChunkDescriptor &chunk = args.chunk_table->chunks[chunk_index];
 
        WordCountMap local = process_chunk_text(chunk.data);
 
        if (!flush_to_pipes(local, args.pipe_first_half[1], args.pipe_second_half[1])) {
            std::cerr << "Worker " << args.worker_id
                      << ": error al escribir a pipes\n";
        }
 
        chunk.processed = true;
    }
}
 
 
/* ─── ReducerArgs ─────────────────────────────────────────────── */
 
struct ReducerArgs {
    int consumer_id;
    int read_fd;
};
 
 
/* ─── hilo consumidor (reducer) ───────────────────────────────── */
 
void reducer_thread(ReducerArgs args) {
    FILE *pipe_stream = fdopen(args.read_fd, "r");
    if (!pipe_stream) {
        std::perror("fdopen");
        return;
    }
 
    WordCountMap final_table;
 
    char line[512];
    while (std::fgets(line, sizeof(line), pipe_stream)) {
        std::string_view sv(line);
 
        // quitar '\n'
        auto nl = sv.find('\n');
        if (nl != std::string_view::npos)
            sv.remove_suffix(sv.size() - nl);
 
        auto sep = sv.find('|');
        if (sep == std::string_view::npos) {
            std::cerr << "Reducer " << args.consumer_id
                      << ": línea inválida: " << sv << '\n';
            continue;
        }
 
        std::string word(sv.substr(0, sep));
        int count = std::stoi(std::string(sv.substr(sep + 1)));
 
        final_table[word] += count;
    }
 
    std::cout << "\n--- pipe " << args.consumer_id << " ---\n";
    for (auto &[word, count] : final_table)
        std::cout << word << " : " << count << '\n';
 
    std::fclose(pipe_stream);
}
 
 
/* ─── main ────────────────────────────────────────────────────── */
 
int main() {
    /* crear pipes */
    int pipe_first_half[2];
    int pipe_second_half[2];
 
    if (pipe(pipe_first_half) == -1) {
        std::perror("pipe_first_half");
        return 1;
    }
    if (pipe(pipe_second_half) == -1) {
        std::perror("pipe_second_half");
        return 1;
    }
 
    /* bounded buffer */
    BoundedBuffer buffer(BUFFER_CAPACITY);
 
    /* leer archivo */
    std::string file_data;
    try {
        file_data = read_file("input.txt");
    } catch (const std::exception &e) {
        std::cerr << e.what() << '\n';
        return 1;
    }
 
    /* construir chunks */
    ChunkTable chunk_table = build_chunks(file_data, NUM_CHUNKS);
    file_data.clear();          // ya no se necesita el buffer completo
 
    /* lanzar reducers */
    std::thread rt0(reducer_thread, ReducerArgs{ 0, pipe_first_half[0]  });
    std::thread rt1(reducer_thread, ReducerArgs{ 1, pipe_second_half[0] });
 
    /* lanzar workers */
    std::vector<std::thread> workers;
    workers.reserve(NUM_WORKERS);
 
    for (int i = 0; i < NUM_WORKERS; ++i) {
        WorkerArgs wa;
        wa.worker_id   = i;
        wa.chunk_table = &chunk_table;
        wa.buffer      = &buffer;
        wa.pipe_first_half[0]  = pipe_first_half[0];
        wa.pipe_first_half[1]  = pipe_first_half[1];
        wa.pipe_second_half[0] = pipe_second_half[0];
        wa.pipe_second_half[1] = pipe_second_half[1];
 
        workers.emplace_back(worker_thread, wa);
    }
 
    /* encolar chunks */
    for (int i = 0; i < static_cast<int>(chunk_table.chunks.size()); ++i)
        buffer.push(i);
 
    /* centinelas: uno por worker */
    for (int i = 0; i < NUM_WORKERS; ++i)
        buffer.push(-1);
 
    /* esperar workers */
    for (auto &w : workers)
        w.join();
 
    /* cerrar extremos de escritura → EOF para reducers */
    close(pipe_first_half[1]);
    close(pipe_second_half[1]);
 
    /* esperar reducers */
    rt0.join();
    rt1.join();
 
    /* cerrar extremos de lectura */
    close(pipe_first_half[0]);
    close(pipe_second_half[0]);
 
    return 0;
}
