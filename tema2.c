#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TRACKER_RANK 0
#define TAG_INIT 1
#define TAG_SWARM 2
#define TAG_FINISH_SINGLE_DOWNLOAD 3
#define TAG_FINISH_ALL_DOWNLOADS 4
#define MESSAGE_TYPE_TERMINATE_ALL_PEERS 5
#define MESSAGE_TYPE_REQUEST_CHUNK 6

#define MAX_FILES 15
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100
#define MAX_PEERS 32

typedef struct {
    char filename[MAX_FILENAME];
    int number_of_chunks;
    char chunk_hashes[MAX_CHUNKS + 1][HASH_SIZE + 1];
    int swarm_size;
    int swarm[MAX_PEERS];
} tracker_file_data;

typedef struct {
    char filename[MAX_FILENAME];
    int number_of_chunks;
    char chunk_hashes[MAX_CHUNKS + 1][HASH_SIZE + 1];
} peer_file_data;

typedef struct {
    int number_of_owned_files;
    peer_file_data owned_files[MAX_FILES];
    int number_of_requested_files;
    char requested_files[MAX_FILES][MAX_FILENAME];
} peer_input_data;

int number_of_files_for_peer[MAX_PEERS];
peer_file_data file_data_for_peer[MAX_PEERS][MAX_FILES];

void read_input_file(int rank, peer_input_data *data) {
    char filename[MAX_FILENAME];
    snprintf(filename, MAX_FILENAME, "in%d.txt", rank);
    FILE *file = fopen(filename, "r");
    if (!file)
        exit(-1);

    fscanf(file, "%d", &data->number_of_owned_files);
    for (int i = 0; i < data->number_of_owned_files; i++) {
        fscanf(file, "%s", data->owned_files[i].filename);
        fscanf(file, "%d", &data->owned_files[i].number_of_chunks);
        for (int j = 0; j < data->owned_files[i].number_of_chunks; j++) {
            fscanf(file, "%s", data->owned_files[i].chunk_hashes[j]);
        }
    }
    fscanf(file, "%d", &data->number_of_requested_files);
    for (int i = 0; i < data->number_of_requested_files; i++) {
        fscanf(file, "%s", data->requested_files[i]);
    }

    fclose(file);
}

void write_output_file(int rank, const char *obtained_file, int number_of_chunks, char hash_list[MAX_CHUNKS + 1][HASH_SIZE + 1]) {
    char filename[MAX_FILENAME];
    snprintf(filename, MAX_FILENAME, "client%d_%s", rank, obtained_file);
    FILE *file = fopen(filename, "w");
    if (!file)
        exit(-1);
        
    for (int i = 0; i < number_of_chunks; i++) {
        fprintf(file, "%s\n", hash_list[i]);
    }

    fclose(file);
}

void *download_thread_func(void *arg) {
    int rank = *(int *)arg;
    MPI_Status status;

    peer_input_data data;
    read_input_file(rank, &data);
    number_of_files_for_peer[rank] = data.number_of_owned_files;
    MPI_Send(&data.number_of_owned_files, 1, MPI_INT, TRACKER_RANK, TAG_INIT, MPI_COMM_WORLD);
    for (int i = 0; i < data.number_of_owned_files; i++) {
        strcpy(file_data_for_peer[rank][i].filename, data.owned_files[i].filename);
        MPI_Send(data.owned_files[i].filename, MAX_FILENAME, MPI_CHAR, TRACKER_RANK, TAG_INIT, MPI_COMM_WORLD);
        file_data_for_peer[rank][i].number_of_chunks = data.owned_files[i].number_of_chunks;
        MPI_Send(&data.owned_files[i].number_of_chunks, 1, MPI_INT, TRACKER_RANK, TAG_INIT, MPI_COMM_WORLD);
        for (int j = 0; j < data.owned_files[i].number_of_chunks; j++) {
            strcpy(file_data_for_peer[rank][i].chunk_hashes[j], data.owned_files[i].chunk_hashes[j]);
            MPI_Send(data.owned_files[i].chunk_hashes[j], HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, TAG_INIT, MPI_COMM_WORLD);
        }
    }
    char message[4];
    MPI_Recv(message, 4, MPI_CHAR, TRACKER_RANK, TAG_INIT, MPI_COMM_WORLD, &status);
    int tracker_message_type, peer_message_type;
    for (int i = 0; i < data.number_of_requested_files; i++) {
        int new_file_id = number_of_files_for_peer[rank];
        number_of_files_for_peer[rank]++;
        tracker_message_type = TAG_SWARM;
        MPI_Send(&tracker_message_type, 1, MPI_INT, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD);
        char *current_requested_file = data.requested_files[i];
        strcpy(file_data_for_peer[rank][new_file_id].filename, current_requested_file);
        MPI_Send(current_requested_file, MAX_FILENAME, MPI_CHAR, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD);
        int number_of_chunks;
        MPI_Recv(&number_of_chunks, 1, MPI_INT, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD, &status);
        char hash_list[MAX_CHUNKS + 1][HASH_SIZE + 1];
        for (int j = 0; j < number_of_chunks; j++) {
            MPI_Recv(hash_list[j], HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD, &status);
        }
        int swarm_size;
        MPI_Recv(&swarm_size, 1, MPI_INT, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD, &status);
        int swarm_list[MAX_PEERS];
        MPI_Recv(swarm_list, MAX_PEERS, MPI_INT, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD, &status);
        for (int j = 0; j < number_of_chunks; j++) {
            if (j + 10 < number_of_chunks) {
                for (int k = 0; k < 10; k++) {
                    int success = 0;
                    while (success == 0) {
                    int peer_rank = swarm_list[rand() % swarm_size];
                        if (peer_rank == rank) {
                            continue;
                        }
                        peer_message_type = MESSAGE_TYPE_REQUEST_CHUNK;
                        MPI_Send(&peer_message_type, 1, MPI_INT, peer_rank, 0, MPI_COMM_WORLD);
                        MPI_Send(hash_list[j], HASH_SIZE + 1, MPI_CHAR, peer_rank, MESSAGE_TYPE_REQUEST_CHUNK + rank, MPI_COMM_WORLD);
                        MPI_Recv(message, 4, MPI_CHAR, peer_rank, MESSAGE_TYPE_REQUEST_CHUNK + rank, MPI_COMM_WORLD, &status);
                        if (strcmp(message, "OKI") == 0) {
                            success = 1;
                            break;
                        }
                    }
                    strcpy(file_data_for_peer[rank][new_file_id].chunk_hashes[j + k], hash_list[j + k]);
                    file_data_for_peer[rank][new_file_id].number_of_chunks++;
                }
                tracker_message_type = TAG_SWARM;
                MPI_Send(&tracker_message_type, 1, MPI_INT, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD);
                MPI_Send(current_requested_file, MAX_FILENAME, MPI_CHAR, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD);
                MPI_Recv(&number_of_chunks, 1, MPI_INT, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD, &status);
                for (int k = 0; k < number_of_chunks; k++) {
                    MPI_Recv(hash_list[k], HASH_SIZE + 1, MPI_CHAR, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD, &status);
                }
                MPI_Recv(&swarm_size, 1, MPI_INT, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD, &status);
                MPI_Recv(swarm_list, MAX_PEERS, MPI_INT, TRACKER_RANK, TAG_SWARM, MPI_COMM_WORLD, &status);
                j = j + 10;
                j--;
            } else {
                int success = 0;
                while (success == 0) {
                    int peer_rank = swarm_list[rand() % swarm_size];
                    if (peer_rank == rank) {
                        continue;
                    }
                    peer_message_type = MESSAGE_TYPE_REQUEST_CHUNK;
                    MPI_Send(&peer_message_type, 1, MPI_INT, peer_rank, 0, MPI_COMM_WORLD);
                    MPI_Send(hash_list[j], HASH_SIZE + 1, MPI_CHAR, peer_rank, MESSAGE_TYPE_REQUEST_CHUNK + rank, MPI_COMM_WORLD);
                    MPI_Recv(message, 4, MPI_CHAR, peer_rank, MESSAGE_TYPE_REQUEST_CHUNK + rank, MPI_COMM_WORLD, &status);
                    if (strcmp(message, "OKI") == 0) {
                        success = 1;
                        break;
                    }
                }
                strcpy(file_data_for_peer[rank][new_file_id].chunk_hashes[j], hash_list[j]);
                file_data_for_peer[rank][new_file_id].number_of_chunks++;
            }
        }
        tracker_message_type = TAG_FINISH_SINGLE_DOWNLOAD;
        MPI_Send(&tracker_message_type, 1, MPI_INT, TRACKER_RANK, TAG_FINISH_SINGLE_DOWNLOAD, MPI_COMM_WORLD);
        write_output_file(rank, current_requested_file, file_data_for_peer[rank][new_file_id].number_of_chunks, file_data_for_peer[rank][new_file_id].chunk_hashes);
    }
    tracker_message_type = TAG_FINISH_ALL_DOWNLOADS;
    MPI_Send(&tracker_message_type, 1, MPI_INT, TRACKER_RANK, TAG_FINISH_ALL_DOWNLOADS, MPI_COMM_WORLD);

    return NULL;
}

void *upload_thread_func(void *arg)
{
    int rank = *(int*) arg;
    MPI_Status status;

    while (1) {
        int peer_message_type, sender_rank;
        MPI_Recv(&peer_message_type, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        sender_rank = status.MPI_SOURCE;
        if (peer_message_type == MESSAGE_TYPE_TERMINATE_ALL_PEERS && sender_rank == TRACKER_RANK) {
            break;
        } else {
            char requested_hash[HASH_SIZE + 1];
            MPI_Recv(requested_hash, HASH_SIZE + 1, MPI_CHAR, sender_rank, MESSAGE_TYPE_REQUEST_CHUNK + sender_rank, MPI_COMM_WORLD, &status);
            int existent_chunk = 0;
            int number_of_peer_files = number_of_files_for_peer[rank];
            for (int i = 0; i < number_of_peer_files; i++) {
                int number_of_chunks = file_data_for_peer[rank][i].number_of_chunks;
                for (int j = 0; j < number_of_chunks; j++) {
                    if (strcmp(file_data_for_peer[rank][i].chunk_hashes[j], requested_hash) == 0) {
                        existent_chunk = 1;
                        break;
                    }
                }
                if (existent_chunk) {
                    break;
                }
            }
            if (existent_chunk) {
                char message[4] = "OKI";
                MPI_Send(message, 4, MPI_CHAR, sender_rank, MESSAGE_TYPE_REQUEST_CHUNK + sender_rank, MPI_COMM_WORLD);
            } else {
                char message[4] = "NOK";
                MPI_Send(message, 4, MPI_CHAR, sender_rank, MESSAGE_TYPE_REQUEST_CHUNK + sender_rank, MPI_COMM_WORLD);
            }
        }
    }

    return NULL;
}

void tracker(int numtasks, int rank) {
    MPI_Status status;

    int peer_status[numtasks];
    peer_status[0] = 0;
    for (int i = 1; i < numtasks; i++)
        peer_status[i] = 1;
    int number_of_files = 0;
    tracker_file_data files[MAX_FILES];
    for (int i = 1; i < numtasks; i++) {
        int number_of_peer_files;
        MPI_Recv(&number_of_peer_files, 1, MPI_INT, i, TAG_INIT, MPI_COMM_WORLD, &status);
        for (int j = 0; j < number_of_peer_files; j++) {
            char filename[MAX_FILENAME];
            MPI_Recv(filename, MAX_FILENAME, MPI_CHAR, i, TAG_INIT, MPI_COMM_WORLD, &status);
            int number_of_chunks;
            MPI_Recv(&number_of_chunks, 1, MPI_INT, i, TAG_INIT, MPI_COMM_WORLD, &status);
            char chunk_hashes[MAX_CHUNKS + 1][HASH_SIZE + 1];
            for (int k = 0; k < number_of_chunks; k++)
                MPI_Recv(chunk_hashes[k], HASH_SIZE + 1, MPI_CHAR, i, TAG_INIT, MPI_COMM_WORLD, &status);
            int file_id = -1;
            for (int k = 0; k < number_of_files; k++) {
                if (strcmp(files[k].filename, filename) == 0) {
                    file_id = k;
                    break;
                }
            }
            if (file_id != -1) {
                files[file_id].swarm[files[number_of_files].swarm_size] = i;
                files[number_of_files].swarm_size++;
            } else {
                strcpy(files[number_of_files].filename, filename);
                files[number_of_files].number_of_chunks = number_of_chunks;
                for (int k = 0; k < number_of_chunks; k++)
                    strcpy(files[number_of_files].chunk_hashes[k], chunk_hashes[k]);
                files[number_of_files].swarm_size = 0;
                files[number_of_files].swarm[files[number_of_files].swarm_size] = i;
                files[number_of_files].swarm_size++;
                number_of_files++;
            }
        }
    }
    for (int i = 1; i < numtasks; i++) {
        char message[4] = "OKI";
        MPI_Send(message, 4, MPI_CHAR, i, TAG_INIT, MPI_COMM_WORLD);
    }
    while (1) {
        int tracker_message_type, peer_rank;
        MPI_Recv(&tracker_message_type, 1, MPI_INT, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
        peer_rank = status.MPI_SOURCE;
        if (tracker_message_type == TAG_SWARM && status.MPI_TAG == TAG_SWARM) {
            char filename[MAX_FILENAME];
            MPI_Recv(filename, MAX_FILENAME, MPI_CHAR, peer_rank, TAG_SWARM, MPI_COMM_WORLD, &status);
            int file_id = -1;
            for (int i = 0; i < number_of_files; i++) {
                if (strcmp(files[i].filename, filename) == 0) {
                    file_id = i;
                    break;
                }
            }
            if (file_id == -1) {
                exit(-1);
            } else {
                MPI_Send(&files[file_id].number_of_chunks, 1, MPI_INT, peer_rank, TAG_SWARM, MPI_COMM_WORLD);
                for (int i = 0; i < files[file_id].number_of_chunks; i++) {
                    MPI_Send(files[file_id].chunk_hashes[i], HASH_SIZE + 1, MPI_CHAR, peer_rank, TAG_SWARM, MPI_COMM_WORLD);
                }
                MPI_Send(&files[file_id].swarm_size, 1, MPI_INT, peer_rank, TAG_SWARM, MPI_COMM_WORLD);
                MPI_Send(files[file_id].swarm, MAX_PEERS, MPI_INT, peer_rank, TAG_SWARM, MPI_COMM_WORLD);
                files[file_id].swarm[files[file_id].swarm_size] = peer_rank;
                files[file_id].swarm_size++;
            }
        } else if (tracker_message_type == TAG_FINISH_SINGLE_DOWNLOAD && status.MPI_TAG == TAG_FINISH_SINGLE_DOWNLOAD) {
            continue;
        } else if (tracker_message_type == TAG_FINISH_ALL_DOWNLOADS && status.MPI_TAG == TAG_FINISH_ALL_DOWNLOADS) {
            peer_status[peer_rank] = 0;
            int all_peers_finished = 1;
            for (int i = 1; i < numtasks; i++) {
                if (peer_status[i] == 1) {
                    all_peers_finished = 0;
                    break;
                }
            }
            if (all_peers_finished) {
                for (int i = 1; i < numtasks; i++) {
                    int peer_message_type = MESSAGE_TYPE_TERMINATE_ALL_PEERS;
                    MPI_Send(&peer_message_type, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
                }
                break;
            }
        }
    }
}

void peer(int numtasks, int rank) {
    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;

    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }
}
 
int main (int argc, char *argv[]) {
    int numtasks, rank;
 
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }

    MPI_Finalize();
}
