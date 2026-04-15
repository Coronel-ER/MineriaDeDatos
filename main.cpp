

#include <stdio.h>      // printf, fprintf, fopen, fclose, fgets, snprintf
#include <stdlib.h>     // malloc, free, calloc, atoi
#include <string.h>     // strlen, strcmp, strdup, memcpy, strchr, strcspn
#include <ctype.h>      // isalnum, tolower
#include <unistd.h>     // pipe, write, close
#include <pthread.h>    // pthread_create, pthread_join, mutex, condvars



/* numero de hilos trabajadores*/
#define NUM_WORKERS 3

/* capacidad máxima del bounded buffer */
#define BUFFER_CAPACITY 8

/* nmero aproximado de chunks en los que se partira el archivo */
#define NUM_CHUNKS 4



/*
 
  cada chunk contiene una copia del texto correspondiente.
 */
typedef struct {
    int chunk_id;         // id del chunk
    char *data;           // texto del chunk en memoria
    size_t len;           // longitud del texto
    size_t start_offset;  // inicio dentro del archivo 
    size_t end_offset;    // final dentro del archivo 
    int processed;        // indicar si ya fue procesado
} ChunkDescriptor;

/*
tabla que almacenara los chunks generados
 */
typedef struct {
    ChunkDescriptor *chunks; // arreglo de chunks
    int num_chunks;          // cantidad de chunks
} ChunkTable;

/*
  bounded buffer, guarda los indices a la tabla de chunks 
 */
typedef struct {
    int *items;               // indices de chunks
    int capacity;             // tamaño maximo del bounded buffer
    int head;                 // cabeza
    int tail;                 // cola
    int count;                // no. actual de elementos

    pthread_mutex_t mutex;    // protege acceso concurrente al buffer
    pthread_cond_t not_empty; // señala que el buffer ya no está vacío
    pthread_cond_t not_full;  // señala que el buffer ya no está lleno
} BoundedBuffer;
-
/*
 linked list para manejar colisiones en la tabla hash
 */
typedef struct WordCountEntry {
    char *word;                    // palabra almacenada
    int count;                     // frecuencia de la palabra
    struct WordCountEntry *next;   // siguiente nodo del bucket
} WordCountEntry;

/*
  tabla hash para conteo de palabras.
 */
typedef struct {
    WordCountEntry **buckets; // arreglo de buckets
    int num_buckets;          // nmero de buckets
} WordCountTable;

/*
 argumentos que recibe cada hilo 
 */
typedef struct {
    int worker_id;              // id
    ChunkTable *chunk_table;    // apuntador a la tabla de chunks
    BoundedBuffer *buffer;      // apuntador al bounded buffer
    int pipe_first_half[2];     // pipa para palabras a-m
    int pipe_second_half[2];    // pipa para palabras n-z
} WorkerArgs;

/*
 argumentos que recibe cada hilo consumidor
 */
typedef struct {
    int consumer_id; // id 
    int read_fd;     // descr. de la lectura de la pipa
} ReducerArgs;



/*
   inicializar el bounded buffer
  reservar memoria para el arreglo de indices e inicializar
  mutex y variables de condición.
 
  returns:
   0  = exito
  -1  = error
 */
int init_bounded_buffer(BoundedBuffer *buffer, int capacity) {
    buffer->items = (int *)malloc(sizeof(int) * capacity);
    if (buffer->items == NULL) {
        return -1;
    }

    buffer->capacity = capacity;
    buffer->head = 0;
    buffer->tail = 0;
    buffer->count = 0;

    if (pthread_mutex_init(&buffer->mutex, NULL) != 0) {
        free(buffer->items);
        return -1;
    }

    if (pthread_cond_init(&buffer->not_empty, NULL) != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        free(buffer->items);
        return -1;
    }

    if (pthread_cond_init(&buffer->not_full, NULL) != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        free(buffer->items);
        return -1;
    }

    return 0;
}

/*
  libera memoria y destruye mutex/condiciones del buffer
 */
void destroy_bounded_buffer(BoundedBuffer *buffer) {
    if (buffer->items != NULL) {
        free(buffer->items);
        buffer->items = NULL;
    }

    pthread_mutex_destroy(&buffer->mutex);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_cond_destroy(&buffer->not_full);
}

/*
  inserta un índice de chunk en el buffer
  si el buffer está lleno, el productor espera.
 */
int push_buffer(BoundedBuffer *buffer, int chunk_index) {
    pthread_mutex_lock(&buffer->mutex);

    /* esperar si el buffer esta lleno */
    while (buffer->count == buffer->capacity) {
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);
    }

    /* insertar elemento en la cola */
    buffer->items[buffer->tail] = chunk_index;
    buffer->tail = (buffer->tail + 1) % buffer->capacity;
    buffer->count++;

    /* avisar al consumidor */
    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);

    return 0;
}

/*
 * consumidor
 * si el buffer esta vacio el consumidor espera
 */
int pop_buffer(BoundedBuffer *buffer) {
    int chunk_index;

    pthread_mutex_lock(&buffer->mutex);

    /* esperar mientras el buffer esta vacio */
    while (buffer->count == 0) {
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    /* extraer elemento de la posicion head */
    chunk_index = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % buffer->capacity;
    buffer->count--;

    /* avisar que ahora hay espacio disponible */
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);

    return chunk_index;
}


/*
 * funcion hash para palabras usando djb2
 */
unsigned long hash_word(const char *str) {
    unsigned long hash = 5381;
    int c;

    while ((c = *str++) != 0) {
        hash = ((hash << 5) + hash) + c; // hash * 33 + c
    }

    return hash;
}

/*
  inicializa una tabla hash de conteo
 */
int init_word_count_table(WordCountTable *table, int num_buckets) {
    table->buckets = (WordCountEntry **)calloc(num_buckets, sizeof(WordCountEntry *));
    if (table->buckets == NULL) {
        return -1;
    }

    table->num_buckets = num_buckets;
    return 0;
}

/*
  crea una nueva entrada de palabra
  el conteo inicial queda en 1
 */
WordCountEntry *create_word_entry(const char *word) {
    WordCountEntry *entry = (WordCountEntry *)malloc(sizeof(WordCountEntry));
    if (entry == NULL) {
        return NULL;
    }

    entry->word = strdup(word);
    if (entry->word == NULL) {
        free(entry);
        return NULL;
    }

    entry->count = 1;
    entry->next = NULL;

    return entry;
}

/*
  inserta una palabra en la tabla hash
  si ya existe, incrementa su contador
 */
int insert_word_count(WordCountTable *table, const char *word) {
    unsigned long hash = hash_word(word);
    int index = (int)(hash % (unsigned long)table->num_buckets);

    WordCountEntry *current = table->buckets[index];

    /* buscar si la palabra ya existe */
    while (current != NULL) {
        if (strcmp(current->word, word) == 0) {
            current->count++;
            return 0;
        }
        current = current->next;
    }

    /* si no existe crear una nueva entrada */
    WordCountEntry *new_entry = create_word_entry(word);
    if (new_entry == NULL) {
        return -1;
    }

    new_entry->next = table->buckets[index];
    table->buckets[index] = new_entry;

    return 0;
}

/*
  inserta una palabra con un valor arbitrario.
  se usa para sumar conteos parciales.
 */
int insert_word_count_by_value(WordCountTable *table, const char *word, int value) {
    unsigned long hash = hash_word(word);
    int index = (int)(hash % (unsigned long)table->num_buckets);

    WordCountEntry *current = table->buckets[index];

    while (current != NULL) {
        if (strcmp(current->word, word) == 0) {
            current->count += value;
            return 0;
        }
        current = current->next;
    }

    WordCountEntry *new_entry = create_word_entry(word);
    if (new_entry == NULL) {
        return -1;
    }

    new_entry->count = value;
    new_entry->next = table->buckets[index];
    table->buckets[index] = new_entry;

    return 0;
}

/*
  imprime toda la tabla hash, orden alfabetico no garantizado
 */
void print_word_count_table(WordCountTable *table) {
    for (int i = 0; i < table->num_buckets; i++) {
        WordCountEntry *current = table->buckets[i];

        while (current != NULL) {
            printf("%s : %d\n", current->word, current->count);
            current = current->next;
        }
    }
}

/*
  libera memoria de la tabla hash
 */
void destroy_word_count_table(WordCountTable *table) {
    if (table->buckets == NULL) {
        return;
    }

    for (int i = 0; i < table->num_buckets; i++) {
        WordCountEntry *current = table->buckets[i];

        while (current != NULL) {
            WordCountEntry *temp = current;
            current = current->next;

            free(temp->word);
            free(temp);
        }
    }

    free(table->buckets);
    table->buckets = NULL;
    table->num_buckets = 0;
}


/*
  leer un archivo completo desde disco a memoria, dvuelve un buffer terminado en '\0'
 */
char *read_file_into_memory(const char *filename, size_t *out_size) {
    FILE *fp = fopen(filename, "rb");
    if (fp == NULL) {
        return NULL;
    }

    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return NULL;
    }

    long file_size = ftell(fp);
    if (file_size < 0) {
        fclose(fp);
        return NULL;
    }

    rewind(fp);

    char *buffer = (char *)malloc((size_t)file_size + 1);
    if (buffer == NULL) {
        fclose(fp);
        return NULL;
    }

    size_t bytes_read = fread(buffer, 1, (size_t)file_size, fp);
    fclose(fp);

    if (bytes_read != (size_t)file_size) {
        free(buffer);
        return NULL;
    }

    buffer[file_size] = '\0';
    *out_size = (size_t)file_size;

    return buffer;
}

/*
  auxiliar para no cortar palabras a la mitad
 */
int is_word_char(char c) {
    return isalnum((unsigned char)c);
}

/*
 liberar memoria de la tabla de chunks
 */
void destroy_chunk_table(ChunkTable *chunk_table) {
    if (chunk_table->chunks == NULL) {
        return;
    }

    for (int i = 0; i < chunk_table->num_chunks; i++) {
        free(chunk_table->chunks[i].data);
    }

    free(chunk_table->chunks);
    chunk_table->chunks = NULL;
    chunk_table->num_chunks = 0;
}

/*
  construir los chunks a partir del archivo en memoria y evitar cortar palabras a la mitad
  
 */
int build_chunks_from_buffer(const char *file_data,size_t file_size, int num_chunks, ChunkTable *chunk_table) {
    chunk_table->chunks = (ChunkDescriptor *)malloc(sizeof(ChunkDescriptor) * num_chunks);
    if (chunk_table->chunks == NULL) {
        return -1;
    }

    chunk_table->num_chunks = 0;

    size_t approx_chunk_size = file_size / (size_t)num_chunks;
    size_t start = 0;

    for (int i = 0; i < num_chunks; i++) {
        if (start >= file_size) {
            break;
        }

        size_t end;

        /* ell ultimo chunk llega hasta el final */
        if (i == num_chunks - 1) {
            end = file_size;
        } else {
            end = start + approx_chunk_size;
            if (end > file_size) {
                end = file_size;
            }

            /*
              si el corte cae en medio de una palabra, avanzar hasta acabarla
            
             */
            while (end < file_size && is_word_char(file_data[end])) {
                end++;
            }
        }

        size_t len = end - start;

        char *chunk_copy = (char *)malloc(len + 1);
        if (chunk_copy == NULL) {
            destroy_chunk_table(chunk_table);
            return -1;
        }

        memcpy(chunk_copy, file_data + start, len);
        chunk_copy[len] = '\0';

        chunk_table->chunks[chunk_table->num_chunks].chunk_id = chunk_table->num_chunks;
        chunk_table->chunks[chunk_table->num_chunks].data = chunk_copy;
        chunk_table->chunks[chunk_table->num_chunks].len = len;
        chunk_table->chunks[chunk_table->num_chunks].start_offset = start;
        chunk_table->chunks[chunk_table->num_chunks].end_offset = end;
        chunk_table->chunks[chunk_table->num_chunks].processed = 0;

        chunk_table->num_chunks++;
        start = end;
    }

    return 0;
}

/*auxiliares de las pipas*/

/*
  d ecidir a que pipa enviar una palabra
   0 = a..m
   1 = n..z
  -1 = no es valida
 */
int classify_word_pipe(const char *word) {
    if (word == NULL || word[0] == '\0') {
        return -1;
    }

    char c = (char)tolower((unsigned char)word[0]);

    if (c >= 'a' && c <= 'm') {
        return 0;
    }

    if (c >= 'n' && c <= 'z') {
        return 1;
    }

    return -1;
}

/*
 * escribir todos los bytes en un file descriptor, se usa para aseguran escrituras completas al pipe
 */
int write_all(int fd, const char *buf, size_t len) {
    size_t total = 0;

    while (total < len) {
        ssize_t n = write(fd, buf + total, len - total);
        if (n <= 0) {
            return -1;
        }
        total += (size_t)n;
    }

    return 0;
}

/*
   envia una tupla palabra-conteo a la pipa
    palabra|conteo\n
 */
int send_word_count_to_pipe(int fd, const char *word, int count) {
    char buffer[512];
    int written = snprintf(buffer, sizeof(buffer), "%s|%d\n", word, count);

    if (written < 0 || written >= (int)sizeof(buffer)) {
        return -1;
    }

    return write_all(fd, buffer, (size_t)written);
}

/*
  parsear una linea del pipe con formato palabra|conteo
 */
int parse_word_count_line(const char *line, char *word_out, size_t word_size, int *count_out) {
    const char *sep = strchr(line, '|');
    if (sep == NULL) {
        return -1;
    }

    size_t word_len = (size_t)(sep - line);
    if (word_len == 0 || word_len >= word_size) {
        return -1;
    }

    memcpy(word_out, line, word_len);
    word_out[word_len] = '\0';

    *count_out = atoi(sep + 1);
    return 0;
}


/*
  tokenizar el texto de un chunk,
    conviertir letras a minusculas
    separar por delimitadores
    contar palabras localmente
 */
int process_chunk_text(const char *data, size_t len, WordCountTable *local_table) {
    char word[256];
    int wlen = 0;

    for (size_t i = 0; i < len; i++) {
        unsigned char ch = (unsigned char)data[i];

        if (isalnum(ch)) {
            if (wlen < (int)(sizeof(word) - 1)) {
                word[wlen++] = (char)tolower(ch);
            }
        } else {
            if (wlen > 0) {
                word[wlen] = '\0';

                if (insert_word_count(local_table, word) != 0) {
                    return -1;
                }

                wlen = 0;
            }
        }
    }

    /* si el chunk termino con una palabra en construccion, agregarla */
    if (wlen > 0) {
        word[wlen] = '\0';

        if (insert_word_count(local_table, word) != 0) {
            return -1;
        }
    }

    return 0;
}

/*
  recorre la tabla local del worker y manda cada entrada a la pipa correspondiente
 */
int flush_local_counts_to_pipes(WordCountTable *table, int pipe_first_fd, int pipe_second_fd) {
    for (int i = 0; i < table->num_buckets; i++) {
        WordCountEntry *current = table->buckets[i];

        while (current != NULL) {
            int bucket = classify_word_pipe(current->word);

            if (bucket == 0) {
                if (send_word_count_to_pipe(pipe_first_fd, current->word, current->count) != 0) {
                    return -1;
                }
            } else if (bucket == 1) {
                if (send_word_count_to_pipe(pipe_second_fd, current->word, current->count) != 0) {
                    return -1;
                }
            }

            current = current->next;
        }
    }

    return 0;
}

/* worker
  saca un chunk del buffer
 si recibe -1 termina
  tokeniza el chunk
 cuenta localmente
 envía conteos parciales a los pipes
 */
void *worker_thread(void *arg) {
    WorkerArgs *args = (WorkerArgs *)arg;

    while (1) {
        int chunk_index = pop_buffer(args->buffer);

        /*  centinela para indicar fin de trabajo */
        if (chunk_index == -1) {
            break;
        }

        ChunkDescriptor *chunk = &args->chunk_table->chunks[chunk_index];

        WordCountTable local_table;
        if (init_word_count_table(&local_table, 101) != 0) {
            fprintf(stderr, "Worker %d: error al inicializar tabla local\n", args->worker_id);
            continue;
        }

        if (process_chunk_text(chunk->data, chunk->len, &local_table) != 0) {
            fprintf(stderr, "Worker %d: error al procesar chunk %d\n",
                    args->worker_id, chunk->chunk_id);
            destroy_word_count_table(&local_table);
            continue;
        }

        if (flush_local_counts_to_pipes(&local_table,
                                        args->pipe_first_half[1],
                                        args->pipe_second_half[1]) != 0) {
            fprintf(stderr, "Worker %d: error al escribir a pipes\n", args->worker_id);
            destroy_word_count_table(&local_table);
            continue;
        }

        chunk->processed = 1;
        destroy_word_count_table(&local_table);
    }

    return NULL;
}


/* consumidor
lee líneas desde un pipe 
parsea palabra|conteo
acumula resultados en una tabla hash final
imprime resultados al terminar
 */
void *reducer_thread(void *arg) {
    ReducerArgs *args = (ReducerArgs *)arg;

    FILE *pipe_stream = fdopen(args->read_fd, "r");
    if (pipe_stream == NULL) {
        perror("fdopen");
        return NULL;
    }

    WordCountTable final_table;
    if (init_word_count_table(&final_table, 211) != 0) {
        fprintf(stderr, "Reducer %d: error al inicializar tabla final\n", args->consumer_id);
        fclose(pipe_stream);
        return NULL;
    }

    char line[512];
    char word[256];
    int count;

    while (fgets(line, sizeof(line), pipe_stream) != NULL) {
        /* quitar salto de linea final */
        line[strcspn(line, "\n")] = '\0';

        if (parse_word_count_line(line, word, sizeof(word), &count) != 0) {
            fprintf(stderr, "Reducer %d: línea inválida: %s\n", args->consumer_id, line);
            continue;
        }

        if (insert_word_count_by_value(&final_table, word, count) != 0) {
            fprintf(stderr, "Reducer %d: error al acumular %s\n", args->consumer_id, word);
        }
    }

    printf("\n pipa %d \n", args->consumer_id);
    print_word_count_table(&final_table);

    destroy_word_count_table(&final_table);
    fclose(pipe_stream);

    return NULL;
}

/* 
main  */

int main(void) {
    /* pipas para separar palabras por rango alfabetico */
    int pipe_first_half[2];
    int pipe_second_half[2];

    /* crear pipas */
    if (pipe(pipe_first_half) == -1) {
        perror("pipe_first_half");
        return 1;
    }

    if (pipe(pipe_second_half) == -1) {
        perror("pipe_second_half");
        return 1;
    }

    /* inicializar el bounded buffer */
    BoundedBuffer buffer;
    if (init_bounded_buffer(&buffer, BUFFER_CAPACITY) != 0) {
        fprintf(stderr, "Error al inicializar bounded buffer\n");
        return 1;
    }

    /* leer archivo completo de disco a memoria */
    size_t file_size = 0;
    char *file_data = read_file_into_memory("input.txt", &file_size);
    if (file_data == NULL) {
        fprintf(stderr, "Error al leer input.txt\n");
        destroy_bounded_buffer(&buffer);
        return 1;
    }

    /* construir chunks sin romper palabras */
    ChunkTable chunk_table;
    if (build_chunks_from_buffer(file_data, file_size, NUM_CHUNKS, &chunk_table) != 0) {
        fprintf(stderr, "Error al construir chunks\n");
        free(file_data);
        destroy_bounded_buffer(&buffer);
        return 1;
    }

    /* el buffer del archivo completo ya no se necesita */
    free(file_data);

    /* crear consumidores */
    pthread_t reducer_threads[2];
    ReducerArgs reducer_args[2];

    reducer_args[0].consumer_id = 0;
    reducer_args[0].read_fd = pipe_first_half[0];

    reducer_args[1].consumer_id = 1;
    reducer_args[1].read_fd = pipe_second_half[0];

    if (pthread_create(&reducer_threads[0], NULL, reducer_thread, &reducer_args[0]) != 0) {
        fprintf(stderr, "Error al crear reducer 0\n");
        destroy_chunk_table(&chunk_table);
        destroy_bounded_buffer(&buffer);
        return 1;
    }

    if (pthread_create(&reducer_threads[1], NULL, reducer_thread, &reducer_args[1]) != 0) {
        fprintf(stderr, "Error al crear reducer 1\n");
        destroy_chunk_table(&chunk_table);
        destroy_bounded_buffer(&buffer);
        return 1;
    }

    /* crear trabajadores */
    pthread_t worker_threads[NUM_WORKERS];
    WorkerArgs worker_args[NUM_WORKERS];

    for (int i = 0; i < NUM_WORKERS; i++) {
        worker_args[i].worker_id = i;
        worker_args[i].chunk_table = &chunk_table;
        worker_args[i].buffer = &buffer;
        worker_args[i].pipe_first_half[0] = pipe_first_half[0];
        worker_args[i].pipe_first_half[1] = pipe_first_half[1];
        worker_args[i].pipe_second_half[0] = pipe_second_half[0];
        worker_args[i].pipe_second_half[1] = pipe_second_half[1];

        if (pthread_create(&worker_threads[i], NULL, worker_thread, &worker_args[i]) != 0) {
            fprintf(stderr, "Error al crear worker %d\n", i);
            destroy_chunk_table(&chunk_table);
            destroy_bounded_buffer(&buffer);
            return 1;
        }
    }

    /* insertar todos los índices de chunks en el buffer */
    for (int i = 0; i < chunk_table.num_chunks; i++) {
        push_buffer(&buffer, i);
    }

    /*
     * insertar un centinela -1 por cada worker para indicar
     * que ya no habrá más trabajo.
     */
    for (int i = 0; i < NUM_WORKERS; i++) {
        push_buffer(&buffer, -1);
    }

    /* esperar a que terminen todos los workers */
    for (int i = 0; i < NUM_WORKERS; i++) {
        pthread_join(worker_threads[i], NULL);
    }

    /*
     cerrar escritura de las pipas
 
     */
    close(pipe_first_half[1]);
    close(pipe_second_half[1]);

    /* esperar a que terminen ambos consumidores */
    pthread_join(reducer_threads[0], NULL);
    pthread_join(reducer_threads[1], NULL);

    /* cerrar lectura */
    close(pipe_first_half[0]);
    close(pipe_second_half[0]);

    /* liberar memoria */
    destroy_chunk_table(&chunk_table);
    destroy_bounded_buffer(&buffer);

    return 0;
}
