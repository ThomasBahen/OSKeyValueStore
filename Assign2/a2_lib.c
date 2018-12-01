#include "a2_lib.h"



int kv_store_create(char *kv_store_name){
	//intialize variables
	int fd;
	char *addr;

	//opens shared memory
	//failure propogates to creation failure
	if ((fd = shm_open(kv_store_name, O_CREAT|O_RDWR, S_IRUSR |S_IWUSR)) == -1){
		printf("shm_open error, kv_store_create");
		return -1;
	}
	//sets size of store to multiple of Key-Value pairs
	ftruncate(fd, __KEY_VALUE_STORE_SIZE__);
	//maps the store to virtual memory
	//failure propogates to creation failure
	//needed to implement bookkeeping in each of the pods
	if((addr = (char*)mmap(NULL, __KEY_VALUE_STORE_SIZE__, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0)) == MAP_FAILED){
		return -1;
	}
	close(fd);
	//save intial address for later
  char *initalAddr = addr;
	//Initalize bookkeeping
	int *currentReaders;
	currentReaders = calloc(1,sizeof(int));

	memcpy(addr, currentReaders, sizeof(int));
	char *startAddr = addr;
	//move address pointer to first pod store more bookkeeping information
	addr+=sizeof(int);



	int i;
	int j;
	//integers to store in each pod
	int **readNext;
	int **spotsLeft;
	int *keysLeft;
	int **oldestValue;
	int *oldestKey;
 	//allocate space for bookkeeping data
	readNext = calloc((KEYS_PER_POD), sizeof(int*));
	for(i=0;i<KEYS_PER_POD;i++){
		readNext[i]= calloc(1,sizeof(int));
	}
	oldestValue = calloc((KEYS_PER_POD), sizeof(int*));
	for(i=0;i<KEYS_PER_POD;i++){
		oldestValue[i]= calloc(1,sizeof(int));
	}

	oldestKey= calloc(1,sizeof(int));

	keysLeft= calloc(1,sizeof(int));
	*keysLeft=KEYS_PER_POD;

	spotsLeft = calloc((KEYS_PER_POD), sizeof(int*));
	for(i=0;i<KEYS_PER_POD;i++){
		spotsLeft[i] = calloc(1,sizeof(int));
		*spotsLeft[i] = VALUES_PER_KEY;
	}




	//iterate through the store to store the pod information
	for(i=0;i<MAX_POD_ENTRY__;i++){
			//copy the bookkeeping into each pod
			for(j=0;j<KEYS_PER_POD;j++){
			memcpy(addr+j*sizeof(int),readNext[j],sizeof(int));
			memcpy(addr+(KEYS_PER_POD)*sizeof(int)+j*sizeof(int),spotsLeft[j],sizeof(int));
			memcpy(addr+2*(KEYS_PER_POD)*sizeof(int)+j*sizeof(int),oldestValue[j],sizeof(int));
		}
			memcpy(addr+KEY_INFO_SIZE,keysLeft,sizeof(int));
			memcpy(addr+KEY_INFO_SIZE+sizeof(int),oldestKey,sizeof(int));
		  addr+=(POD_SIZE);
	}

for(i=0;i<KEYS_PER_POD;i++){
 free(readNext[i]);
 free(spotsLeft[i]);
 free(oldestValue[i]);
}
 free(readNext);
 free(spotsLeft);
 free(oldestValue);
 free(keysLeft);
 free(oldestKey);
 free(currentReaders);

 if(munmap(startAddr,__KEY_VALUE_STORE_SIZE__)==-1){
	 printf("munmap failed");
	 return -1;
 }

 return 0;
}

int kv_store_write(char *key, char *value){
	//intialize variables
  int fd;
	int i;
  int pod_key;
	int position;
	bool repeat = false;
  char *addr;
	char *keyAddr;
	char *trunKey;
  char *valueAddr;
	char *trunValue;
	char *podAddr;
	int **readNext;
	int *keysLeft;
	int **valuesLeft;
	int **oldestValue;
	int *oldestKey;
	//allocate space for bookkeeping data
	readNext = calloc((KEYS_PER_POD), sizeof(int*));
	for(i=0;i<KEYS_PER_POD;i++){
		readNext[i]= calloc(1,sizeof(int));
	}
	oldestValue = calloc((KEYS_PER_POD), sizeof(int*));
	for(i=0;i<KEYS_PER_POD;i++){
		oldestValue[i]= calloc(1,sizeof(int));
	}

	oldestKey= calloc(1,sizeof(int));

	keysLeft= calloc(1,sizeof(int));

	valuesLeft = calloc((KEYS_PER_POD), sizeof(int*));
	for(i=0;i<KEYS_PER_POD;i++){
		valuesLeft[i] = calloc(1,sizeof(int));
	}
  //might create semaphore
  //if creating, sets intial value to 1
  sem_t *thomasBahen_write = sem_open(__KV_WRITERS_SEMAPHORE__, O_CREAT, S_IRWXU, 1);

  //starts the write lock
  sem_wait(thomasBahen_write);

  //failure propogates to creation failure
  if ((fd = shm_open(__KV_STORE_NAME__, O_CREAT|O_RDWR, S_IRWXU)) == -1){
		printf("shm_open error");
    return -1;
  }

	//map the entire kv store
  if((addr =(char*) mmap(NULL, __KEY_VALUE_STORE_SIZE__, PROT_WRITE|PROT_READ, MAP_SHARED, fd, 0)) == MAP_FAILED){
		printf("intial mmap error");
		return -1;
  }
	close(fd);

	//truncate the key and value
	trunKey = calloc(MAX_KEY_SIZE__,sizeof(char));
	trunValue = calloc(MAX_DATA_LENGTH__,sizeof(char));
	keyAddr = calloc(MAX_KEY_SIZE__,sizeof(char));

	strncpy(trunKey,key,MAX_KEY_SIZE__);
	strncpy(trunValue,value,MAX_DATA_LENGTH__);
	trunValue[MAX_DATA_LENGTH__-1]='\0';
	trunKey[MAX_KEY_SIZE__-1]='\0';



  //hash the key Value and find the pod_key
  pod_key = (int)(generate_hash((unsigned char*)trunKey) % MAX_POD_ENTRY__);

	//navigate to the pod based on pod key
	podAddr = addr + pod_key*(POD_SIZE)+sizeof(int);
	char* initalAddr = podAddr;
	//copy pod information into pointers
	for(int i=0;i<KEYS_PER_POD;i++){
  memcpy(readNext[i],podAddr+i*sizeof(int),sizeof(int));
	memcpy(valuesLeft[i],podAddr+(KEYS_PER_POD) * sizeof(int)+i*sizeof(int),sizeof(int));
	memcpy(oldestValue[i],podAddr+2*((KEYS_PER_POD) * sizeof(int))+i*sizeof(int), sizeof(int));
}

	memcpy(keysLeft,podAddr+3*((KEYS_PER_POD) * sizeof(int)),sizeof(int));
	memcpy(oldestKey,podAddr+3* ((KEYS_PER_POD) * sizeof(int))+ sizeof(int),sizeof(int));

	//don't need info section anymore
	podAddr=podAddr+POD_INFO_SIZE;


	for(i=0;i<KEYS_PER_POD;i++){
		//get next key address
		memcpy(keyAddr,podAddr+(i*MAX_KEY_SIZE__),MAX_KEY_SIZE__);
		if(strcmp(trunKey,keyAddr) == 0){
			repeat=true;
			position=i;
		}
	}

	if(repeat){
		//set podAddr to look at the start of the key's subpod
		podAddr=podAddr+(MAX_KEY_SIZE__*KEYS_PER_POD)+(position*(MAX_DATA_LENGTH__*VALUES_PER_KEY));
		if(*valuesLeft[position]>0){
			//still value space for that given key
			memcpy(podAddr+MAX_DATA_LENGTH__*(VALUES_PER_KEY-(*valuesLeft[position])),value,MAX_DATA_LENGTH__);
			(*valuesLeft[position])--;
		}else{
			//overwrite the oldest value associated with key
			memcpy(podAddr+MAX_DATA_LENGTH__*(*oldestValue[position]),trunValue,MAX_DATA_LENGTH__);
			//update the oldest value
			(*oldestValue[position])++;
			if(*oldestValue[position]==KEYS_PER_POD){
				(*oldestValue[position])=0;
			}

		}
	}else{
			//the key is not a repeat
			if(*keysLeft>0){

				keyAddr= podAddr+(KEYS_PER_POD-(*keysLeft))*MAX_KEY_SIZE__;
			 	memcpy(podAddr+(KEYS_PER_POD-(*keysLeft))*MAX_KEY_SIZE__,trunKey,MAX_KEY_SIZE__);
				//point to to the subpod for the new key
				valueAddr=podAddr+(MAX_KEY_SIZE__*KEYS_PER_POD)+(KEYS_PER_POD-(*keysLeft))*MAX_DATA_LENGTH__*VALUES_PER_KEY;

				//copy in the value that must be first
				podAddr = memcpy(podAddr+(MAX_KEY_SIZE__*KEYS_PER_POD)+(KEYS_PER_POD-(*keysLeft))*MAX_DATA_LENGTH__*VALUES_PER_KEY,trunValue+1,MAX_DATA_LENGTH__);
				//there are keys left still

				//now we can copy in the value
				(*keysLeft)--;
			}else{
				//there are no more keys
				memcpy(podAddr+(*oldestKey)*MAX_KEY_SIZE__,trunKey,MAX_KEY_SIZE__);
				//overwrite the old key's bookkeeping
				position=*oldestKey;
				*valuesLeft[*oldestKey]=VALUES_PER_KEY;
				*oldestValue[*oldestKey]=0;
				//update the oldest key
				(*oldestKey)++;
				if(*oldestKey==KEYS_PER_POD){
					(*oldestKey)=0;
				}

				//write the value to the first position of the key's subpod
				valueAddr=podAddr+(MAX_KEY_SIZE__*KEYS_PER_POD)+((position)*(MAX_DATA_LENGTH__*VALUES_PER_KEY));
				memcpy(valueAddr,trunValue,MAX_DATA_LENGTH__);
			}
		}
	//put the bookkeeping back into the store
	for(i=0;i<KEYS_PER_POD;i++){
	memcpy(initalAddr+i*sizeof(int),readNext[i],sizeof(int));
  memcpy(initalAddr+(KEYS_PER_POD) * sizeof(int)+i*sizeof(int),valuesLeft[i],sizeof(int));
	memcpy(initalAddr+2*((KEYS_PER_POD) * sizeof(int))+i*sizeof(int),oldestValue[i], sizeof(int));
}
 	memcpy(initalAddr+3*(KEYS_PER_POD * sizeof(int)),keysLeft,sizeof(int));
	memcpy(initalAddr+3*((KEYS_PER_POD) * sizeof(int))+ sizeof(int),oldestKey,sizeof(int));

	for(i=0;i<KEYS_PER_POD;i++){
		free(readNext[i]);
		free(valuesLeft[i]);
		free(oldestValue[i]);
	}
  free(readNext);
	free(valuesLeft);
	free(oldestValue);
	free(keysLeft);
	free(oldestKey);
	free(trunKey);
	if(munmap(addr,__KEY_VALUE_STORE_SIZE__)==-1){
		printf("munmap failed");
		return -1;
	}

	if(sem_post(thomasBahen_write)==-1){
		return -1;
	}

	sem_close(thomasBahen_write);

return 0;
}






char *kv_store_read(char *key){
	// implement your create method code here
	int position;
	int fd;
	int i;
	int pod_key;
	bool exists =false;
	int *currentReaders;
	char *valueAddr;
	char *trunKey;
	char *addr;
	char *podAddr;
	char *keyAddr;
	int **readNext;
	int *keysLeft;
	int **valuesLeft;
	int **oldestValue;
	int *oldestKey;

	//allocate space for bookkeeping data
	readNext = calloc((KEYS_PER_POD), sizeof(int*));
	for(i=0;i<KEYS_PER_POD;i++){
		readNext[i]= calloc(1,sizeof(int));
	}
	oldestValue = calloc((KEYS_PER_POD), sizeof(int*));
	for(i=0;i<KEYS_PER_POD;i++){
		oldestValue[i]= calloc(1,sizeof(int));
	}
	valueAddr=calloc(MAX_DATA_LENGTH__,sizeof(char));
	keyAddr= calloc(MAX_KEY_SIZE__,sizeof(char));


	oldestKey= calloc(1,sizeof(int));
  currentReaders = calloc(1,sizeof(int));
	keysLeft= calloc(1,sizeof(int));

	valuesLeft = calloc((KEYS_PER_POD), sizeof(int*));
	for(i=0;i<KEYS_PER_POD;i++){
		valuesLeft[i] = calloc(1,sizeof(int));
	}

	//if creating, sets intial value to 1
  sem_t *thomasBahen_writer= sem_open(__KV_WRITERS_SEMAPHORE__, O_CREAT|O_RDWR, S_IRWXU, 1);
	sem_t *thomasBahen_read = sem_open(__KV_READERS_SEMAPHORE__,O_CREAT|O_RDWR,S_IRWXU, 1);

	sem_wait(thomasBahen_read);
	//map the entire kv store
	if ((fd = shm_open(__KV_STORE_NAME__, O_CREAT|O_RDWR, S_IRWXU)) == -1){
		printf("shm_open error");
		return NULL;
	}

  if((addr =(char*) mmap(NULL, __KEY_VALUE_STORE_SIZE__, PROT_WRITE|PROT_READ, MAP_SHARED, fd, 0)) == MAP_FAILED){
		printf("intial mmap error");
		return (char*) KV_EXIT_FAILURE;
  }
	close(fd);
	//get the readerInformation
	memcpy(currentReaders,addr,sizeof(int));
	(*currentReaders)++;
	if(*currentReaders==1){
		//first reader, stop writing
		sem_wait(thomasBahen_writer);
	}
	sem_post(thomasBahen_read);

	//truncate the key
	trunKey = calloc(MAX_KEY_SIZE__,sizeof(char));
	strncpy(trunKey,key,MAX_KEY_SIZE__);
	trunKey[MAX_KEY_SIZE__-1]='\0';
  //hash the key Value and find the pod_key
  pod_key = (int)(generate_hash((unsigned char*)trunKey) % MAX_POD_ENTRY__);

	//navigate to the pod based on pod key
	podAddr = addr + pod_key*(POD_SIZE)+sizeof(int);
	char* initalAddr = podAddr;

	//copy pod information into pointers
	for(i=0;i<KEYS_PER_POD;i++){
	memcpy(readNext[i],podAddr+i*sizeof(int),sizeof(int));
	memcpy(valuesLeft[i],podAddr+(KEYS_PER_POD) * sizeof(int)+i*sizeof(int),sizeof(int));
	memcpy(oldestValue[i],podAddr+2*((KEYS_PER_POD) * sizeof(int))+i*sizeof(int),sizeof(int));
}

	memcpy(keysLeft,podAddr+3*((KEYS_PER_POD) * sizeof(int)),sizeof(int));
	memcpy(oldestKey,podAddr+3*((KEYS_PER_POD) * sizeof(int))+ sizeof(int),sizeof(int));




	for(i=0;i<KEYS_PER_POD;i++){
		//get next key address
		memcpy(keyAddr,podAddr+POD_INFO_SIZE+(i*MAX_KEY_SIZE__),MAX_KEY_SIZE__);
		if(strcmp(trunKey,keyAddr) == 0){
			exists=true;
			position=i;
		}
	}


	//don't need info section anymore
	podAddr=podAddr+POD_INFO_SIZE;

	if(!exists){
		// the key is not in the store
		valueAddr=NULL;
	}else{
		//the key is in the store
		//set podAddr to look at the start of the key's subpod of values
		podAddr=podAddr+(MAX_KEY_SIZE__*KEYS_PER_POD)+(position*(MAX_DATA_LENGTH__*VALUES_PER_KEY));

		//goes to the value based on readNext and reads the value at that address
		podAddr=podAddr+*readNext[position]*MAX_DATA_LENGTH__;
		memcpy(valueAddr,podAddr,MAX_DATA_LENGTH__);
		//update readNext
		(*readNext[position])++;
		if((*readNext[position])==(VALUES_PER_KEY-*valuesLeft[position])){
			(*readNext[position])=0;
		}
	}

	//put the bookkeeping back into the store
	for(i=0;i<KEYS_PER_POD;i++){
	memcpy(initalAddr+i*sizeof(int),readNext[i],sizeof(int));
	memcpy(initalAddr+(KEYS_PER_POD) * sizeof(int)+i*sizeof(int),valuesLeft[i], sizeof(int));
	memcpy(initalAddr+2*((KEYS_PER_POD) * sizeof(int))+i*sizeof(int),oldestValue[i],sizeof(int));
}
	memcpy(initalAddr+3*((KEYS_PER_POD) * sizeof(int)),keysLeft,sizeof(int));
	memcpy(initalAddr+3*((KEYS_PER_POD) * sizeof(int))+ sizeof (int),oldestKey,sizeof(int));

	free(oldestKey);
	free(keysLeft);
	free(keyAddr);
	for(i=0;i<KEYS_PER_POD;i++){
		free(readNext[i]);
		free(valuesLeft[i]);
		free(oldestValue[i]);
	}
	free(readNext);
	free(valuesLeft);
	free(oldestValue);
  free(trunKey);
	if(munmap(addr,__KEY_VALUE_STORE_SIZE__)==-1){
		printf("munmap failed");
		return (char*) KV_EXIT_FAILURE;
	}


	//post semaphores
	sem_wait(thomasBahen_read);
	(*currentReaders)--;
	if(*currentReaders==0){
		sem_post(thomasBahen_writer);
	}
	sem_post(thomasBahen_read);
	free(currentReaders);

	return valueAddr;
}

char **kv_store_read_all(char *key){
	int fd;
	int pod_key;
	int position;
	int i;
	bool exists =false;
	int *currentReaders;
	char **valueAddr = NULL;
  char *trunKey;
	char *podAddr;
	char *keyAddr;
	int **readNext;
	int *keysLeft;
	int **valuesLeft;
	int **oldestValue;
	int *oldestKey;

	//allocate space for bookkeeping data
	readNext = calloc((KEYS_PER_POD), sizeof(int*));
	for(i=0;i<KEYS_PER_POD;i++){
		readNext[i]= calloc(1,sizeof(int));
	}
	oldestValue = calloc((KEYS_PER_POD), sizeof(int*));
	for(i=0;i<KEYS_PER_POD;i++){
		oldestValue[i]= calloc(1,sizeof(int));
	}

	oldestKey= calloc(1,sizeof(int));
	currentReaders = calloc(1,sizeof(int));
	keyAddr = calloc(MAX_KEY_SIZE__,sizeof(char));

	keysLeft= calloc(1,sizeof(int));

	valuesLeft = calloc((KEYS_PER_POD), sizeof(int*));
	for(i=0;i<KEYS_PER_POD;i++){
		valuesLeft[i] = calloc(1,sizeof(int));
	}

	//if creating, sets intial value to 1
	sem_t *thomasBahen_writer= sem_open(__KV_WRITERS_SEMAPHORE__, O_CREAT, S_IRWXU, 1);
	sem_t *thomasBahen_read = sem_open(__KV_READERS_SEMAPHORE__,O_CREAT,S_IRWXU, 1);

	sem_wait(thomasBahen_read);
	//map the entire kv store
	if ((fd = shm_open(__KV_STORE_NAME__, O_CREAT|O_RDWR, S_IRWXU)) == -1){
		printf("shm_open error");
		return (char**) KV_EXIT_FAILURE;
	}

  char *addr = mmap(NULL, __KEY_VALUE_STORE_SIZE__, PROT_WRITE|PROT_READ, MAP_SHARED, fd, 0);

	close(fd);
	//get the readerInformation
	memcpy(currentReaders,addr,sizeof(int));
	(*currentReaders)++;
	if(*currentReaders==1){
		//first reader, stop writing
		sem_wait(thomasBahen_writer);
	}
	sem_post(thomasBahen_read);

	//truncate the key
	trunKey = calloc(MAX_KEY_SIZE__,sizeof(char));
	strncpy(trunKey,key,MAX_KEY_SIZE__);
	trunKey[MAX_KEY_SIZE__-1]='\0';
	//hash the key Value and find the pod_key
	pod_key = (int)(generate_hash((unsigned char*)trunKey) % MAX_POD_ENTRY__);

	//navigate to the pod based on pod key
	podAddr = addr + pod_key*(POD_SIZE)+INT_SIZE;
	char* initalAddr = podAddr;

	//copy pod information into pointers
	for(i=0;i<KEYS_PER_POD;i++){
	memcpy(readNext[i],podAddr+i*sizeof(int),sizeof(int));
	memcpy(valuesLeft[i],podAddr+(KEYS_PER_POD) * sizeof(int)+i*sizeof(int),sizeof(int));
	memcpy(oldestValue[i],podAddr+2*((KEYS_PER_POD) * sizeof(int))+i*sizeof(int),sizeof(int));
}
	memcpy(keysLeft,podAddr+3*((KEYS_PER_POD) * sizeof(int*)),sizeof(int));
	memcpy(oldestKey,podAddr+3*((KEYS_PER_POD) * sizeof(int*))+ sizeof(int),sizeof(int));

	//don't need info section anymore
	podAddr=podAddr+POD_INFO_SIZE;


	for(i=0;i<KEYS_PER_POD;i++){
		//get next key address
		memcpy(keyAddr,podAddr+(i*MAX_KEY_SIZE__),MAX_KEY_SIZE__);
		if(strcmp(keyAddr,trunKey)){
			exists=true;
			position=i;
		}
	}

	if(!exists){
		//the key is not in the store
	}{
		//the key is in the store
		//set podAddr to look at the start of the key's subpod of values
		podAddr=podAddr+(MAX_KEY_SIZE__*KEYS_PER_POD)+(position*(MAX_DATA_LENGTH__*VALUES_PER_KEY));

		//get all values assoc with the key
		for(i=0;i<(VALUES_PER_KEY-(*valuesLeft[position]));i++){
				//copy the value into double ptr
				memcpy(valueAddr[i],podAddr+i*(MAX_DATA_LENGTH__),MAX_DATA_LENGTH__);
		}

	}

	//put the bookkeeping back into the store
	for(i=0;i<KEYS_PER_POD;i++){
	memcpy(initalAddr+i*sizeof(int),readNext[i], sizeof(int));
	memcpy(initalAddr+(KEYS_PER_POD) * sizeof(int)+i*sizeof(int),valuesLeft[i],sizeof(int));
	memcpy(initalAddr+2*((KEYS_PER_POD) * sizeof(int))+i*sizeof(int),oldestValue[i],sizeof(int));
}
	memcpy(initalAddr+3*((KEYS_PER_POD) * sizeof(int)),keysLeft,sizeof(int));
	memcpy(initalAddr+3*((KEYS_PER_POD) * sizeof(int))+ sizeof (int),oldestKey,sizeof(int));



	if(munmap(addr,__KEY_VALUE_STORE_SIZE__)==-1){
		printf("munmap failed");
		return (char**) KV_EXIT_FAILURE;
	}
	for(i=0;i<KEYS_PER_POD;i++){
		free(readNext[i]);
		free(valuesLeft[i]);
		free(oldestValue[i]);
	}
	free(readNext);
	free(valuesLeft);
	free(oldestValue);
  free(keysLeft);
	free(oldestKey);
	free(keyAddr);
	//post semaphores
	sem_wait(thomasBahen_read);
	(*currentReaders)--;
	if(*currentReaders==0){
		sem_post(thomasBahen_writer);
	}
	sem_post(thomasBahen_read);
	free(currentReaders);

	sem_close(thomasBahen_read);
	sem_close(thomasBahen_writer);

	return valueAddr;
}


/* -------------------------------------------------------------------------------
	MY MAIN:: Use it if you want to test your impementation (the above methods)
	with some simple tests as you go on implementing it (without using the tester)
	------------------------------------------------------------------------------- */

/*int main() {
	kv_store_create(__KV_STORE_NAME__);
	kv_store_write("YGqkJkBFyasQbecfNErqWYbtTJgzGhLE","76DD4BF63BE35B07998927E9E09F6AFBF8ACD20BC43BE563EDCC6007F64BEE69");
	kv_store_read("YGqkJkBFyasQbecfNErqWYbtTJgzGhLE");
	return EXIT_SUCCESS;
} */
