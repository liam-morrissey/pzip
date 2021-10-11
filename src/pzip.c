#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pzip.h"

struct arguments {
	int n_threads;
	int thread_num;
	char *input_chars;
	int input_chars_size;
	struct zipped_char *zipped_chars;
	int *zipped_chars_count;
	int *char_frequency;
	pthread_barrier_t *barrier;
	int *local_zip_size;
	pthread_mutex_t *lock1;
	pthread_mutex_t *lock2;

};

void* localThread(void *);

void* localThread(void *argument){
	struct arguments args = *(struct arguments*)(argument); 
	int sector_start= args.input_chars_size/args.n_threads*args.thread_num;
	int sector_end;
	if(args.n_threads-1 == args.thread_num) {
		sector_end = args.input_chars_size;
	}
	else {
		sector_end = args.input_chars_size/args.n_threads +sector_start;
	}
	//obviously inefficient, but then no reallocs required
	if(0 != pthread_mutex_lock(args.lock1)){
		perror("pthread_mutex_lock");
		exit(EXIT_FAILURE);
	}
	struct zipped_char *local_result = malloc(sizeof(struct zipped_char) * (sector_end-sector_start));
	if(0 != pthread_mutex_unlock(args.lock1)){
		perror("pthread_mutex_lock");
		exit(EXIT_FAILURE);
	}
	int local_count = 0;
	char prior_char = args.input_chars[sector_start];
	int char_count = 1;
	//LOCK
	if(0 != pthread_mutex_lock(args.lock2)){
		perror("pthread_mutex_lock");
		exit(EXIT_FAILURE);
	}
	args.char_frequency[args.input_chars[sector_start]-97]++;
	if(0 != pthread_mutex_unlock(args.lock2)) {
		perror("pthread_mutex_unlock");
		exit(EXIT_FAILURE);
	}
	for(int i = sector_start+1; i< sector_end; i++){
		if(args.input_chars[i] == prior_char){
			char_count++;
		}
		else{
			struct zipped_char curr_zip;
			curr_zip.character = prior_char;
			curr_zip.occurence = (uint8_t) char_count;
			//append zipped_char to local array and increment
			local_result[local_count] = curr_zip;
			local_count++;
			char_count = 1;
			prior_char = args.input_chars[i];
		}
		//LOCK
		if(0 != pthread_mutex_lock(args.lock2)){
		perror("pthread_mutex_lock");
		exit(EXIT_FAILURE);
		}
		args.char_frequency[args.input_chars[sector_start]-97]++;
		if(0 != pthread_mutex_unlock(args.lock2)) {
			perror("pthread_mutex_unlock");
			exit(EXIT_FAILURE);
		}
	}
	if(char_count!=1){
		struct zipped_char curr_zip;
		curr_zip.character = prior_char;
		curr_zip.occurence = (uint8_t) char_count;
		//append zipped_char to local array and increment
		local_result[local_count] = curr_zip;
		local_count++;
	}
	args.local_zip_size[args.thread_num] = local_count;

	//SYNCH THREADS
	int ret = pthread_barrier_wait(args.barrier);
	if(0 != ret && ret != PTHREAD_BARRIER_SERIAL_THREAD) {
		perror("pthread_barrier_wait");
		exit(EXIT_FAILURE);
	}
	int start_point=0;
	//find where it should start writing to zip array
	for(int i=0; i<args.thread_num; i++){
		start_point += args.local_zip_size[i];
	}
	//post total zipcount to zipped_chars_count
	if(args.thread_num == args.n_threads-1)
		*args.zipped_chars_count = start_point+local_count;
	//write to global array locations
	for(int i=0; i<local_count; i++) {
		int zip_char_loc = i+start_point;
		args.zipped_chars[zip_char_loc] = local_result[i];
	}
	free(local_result);
	return (void *)0;
}

/**
 * pzip() - zip an array of characters in parallel
 *
 * Inputs:
 * @n_threads:		   The number of threads to use in pzip
 * @input_chars:		   The input characters (a-z) to be zipped
 * @input_chars_size:	   The number of characaters in the input file
 *
 * Outputs:
 * @zipped_chars:       The array of zipped_char structs
 * @zipped_chars_count:   The total count of inserted elements into the zippedChars array.
 * @char_frequency[26]: Total number of occurences
 *
 * NOTE: All outputs are already allocated. DO NOT MALLOC or REASSIGN THEM !!!
 *
 */
void pzip(int n_threads, char *input_chars, int input_chars_size,
	  struct zipped_char *zipped_chars, int *zipped_chars_count,
	  int *char_frequency)
{
	//establish barrier
	pthread_barrier_t barrier;
	if (0 != pthread_barrier_init(&barrier, NULL, n_threads)) {
		perror("ptread_barrier_init");
		exit(EXIT_FAILURE);
	}

	//establish array of thread ids to be used for create and join
	pthread_t *thread_id = malloc(sizeof(pthread_t)*n_threads);

	//create mutex lock and initialize it
	pthread_mutex_t lock1;
	if (0 != pthread_mutex_init(&lock1,NULL)){
		perror("pthread_mutex_init");
		exit(EXIT_FAILURE);
	}
	pthread_mutex_t lock2;
	if (0 != pthread_mutex_init(&lock2,NULL)){
		perror("pthread_mutex_init");
		exit(EXIT_FAILURE);
	}

	int *local_zip_size = malloc(sizeof(int)*n_threads);
	struct arguments *args = malloc(sizeof(struct arguments) *n_threads);
	for( int i=0; i<n_threads; i++) {
		struct arguments *curr_arg = args+i;
		curr_arg->n_threads = n_threads;
		curr_arg->input_chars = input_chars;
		curr_arg->input_chars_size = input_chars_size;
		curr_arg->zipped_chars = zipped_chars;
		curr_arg->zipped_chars_count = zipped_chars_count;
		curr_arg->char_frequency = char_frequency;
		curr_arg->thread_num = i;
		curr_arg->barrier = &barrier;
		curr_arg->local_zip_size = local_zip_size;
		curr_arg->lock1 = &lock1;
		curr_arg->lock2 = &lock2;
		//
		
		if(0 != pthread_create(thread_id+i, NULL, localThread, (void *)(curr_arg))) {
			perror("pthread_create");
			exit(EXIT_FAILURE);
		}
	}
	//Block until all threads have completed runtime
	for(int i=0; i<n_threads; i++){
		void *ret;
		if (0 != pthread_join(thread_id[i],&ret)) {
			perror("pthread_join");
			exit(EXIT_FAILURE);
		}
		if((long)ret!=0){
			fprintf(stderr,"Error in thread %lu",(unsigned long)thread_id[i]);
			exit(EXIT_FAILURE);
		}
	}
	if(0 != pthread_mutex_destroy(&lock1)) {
		perror("pthread_mutex_destroy");
		exit(EXIT_FAILURE);
	}

	if(0 != pthread_mutex_destroy(&lock2)) {
		perror("pthread_mutex_destroy");
		exit(EXIT_FAILURE);
	}

	if(0 != pthread_barrier_destroy(&barrier)) {
		perror("pthread_barrier_destroy");
		exit(EXIT_FAILURE);
	}
	for(int i=0;i<*zipped_chars_count;i++){
		printf("[%c,%d] ",zipped_chars[i].character,zipped_chars[i].occurence);
	}

	free(thread_id);
	free(local_zip_size);
	free(args);

}
