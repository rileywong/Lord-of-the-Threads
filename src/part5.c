#include "lott.h"
#include "extra5.h"
#include <sys/file.h>
#include <pthread.h>
 #include <sys/epoll.h>
#include <sys/socket.h>

static void* map(void*);
static void* reduce(void*);



DIR *dir;
struct dirent *file1;
pthread_mutex_t test;
pthread_mutex_t fileLock;
FILE *temp;

sem_t writeLock;
sem_t readLock;

double maxUserRed;
double minUserRed5 = 99999999;

double maxDurRed;
double minDurRed5 = 99999999;

int countryCount;

char * ccRedPath;

char * maxUpathRed;
char * minUpathRed;

char * maxDpathRed;
char * minDpathRed;

int boolean5 =1;


struct illCount{
    int counter;
}typedef illCount;


static void printer(void * not){
    printf(
        "Part: %s\n"
        "Query: %s\n",

        PART_STRINGS[current_part], QUERY_STRINGS[current_query]);

    if((strcmp(QUERY_STRINGS[current_query],"A") ==0)){
       // printf("%s\n", "This is A" );
        printf("Result %f, %s\n",maxDurRed,maxDpathRed ); 
    }
    if((strcmp(QUERY_STRINGS[current_query],"B") ==0)){
        //printf("%s\n", "This is B" );
        printf("Result %f, %s\n",minDurRed5,minDpathRed ); 
    }
    if((strcmp(QUERY_STRINGS[current_query],"C") ==0)){
        //printf("%s\n", "This is C" );
        printf("Result %f, %s\n",maxUserRed,maxUpathRed ); 
    }
    if((strcmp(QUERY_STRINGS[current_query],"D") ==0)){
        //printf("%s\n", "This is D" );
        printf("Result %f, %s\n",minUserRed5,minUpathRed ); 
    }
    if((strcmp(QUERY_STRINGS[current_query],"E") ==0)){
        //printf("%s\n", "This is E" );
         printf("Result %d, %s\n",findLargestCountryIntRed5(),ccRedPath ); 
    }

}

int part5(size_t nthreads) {
    maxUpathRed = strdup("ZZZZ");
    minUpathRed = strdup("ZZZZ");

    maxDpathRed = strdup("ZZZZ");
    minDpathRed = strdup("ZZZZ");

    minUserRed5 = 999999999;
    minDurRed5 = 999999999;
   
    pthread_mutex_init(&test,NULL);
    pthread_mutex_init(&headProtection,NULL);
    pthread_mutex_init(&fileLock,NULL);

    sem_init(&readLock,0,0);
    sem_init(&writeLock,0,1);

    if((dir = opendir(DATA_DIR))==NULL){
        //printf("Could not open : %s\n",dir );
        printf("%s\n","file cant be found" );
        return -1;
    }


    


    int totalFiles = nfiles5(DATA_DIR);
    pthread_t thread[totalFiles];

    int divider = 0;
    int remainder = 0;
    int difference = 0;


    divider = totalFiles/nthreads;
    remainder = totalFiles - (divider * nthreads);

    if(totalFiles < nthreads){
    difference = totalFiles -remainder;
    }
    else{
        difference = nthreads -remainder;
    }

    
 

    int outerLoop;
 
  int lastLoop = 0;
     
     illCount * counter1 = malloc(sizeof(illCount));
     counter1->counter = divider + 1;

     illCount * counter5 = malloc(sizeof(illCount));
     counter5->counter = divider;

   // printf("%s\n","here?" );

     for(outerLoop = 0 ; outerLoop < remainder; outerLoop++){
      
        pthread_create(&thread[outerLoop],NULL, map,counter1);  
       
     }

         
    for(lastLoop = 0; lastLoop < difference; lastLoop++){ 
       
        pthread_create(&thread[lastLoop+remainder],NULL, map,counter5);  
     }
     
       pthread_t redThread;
    pthread_create(&redThread,NULL,reduce,NULL);


    int x;
    for(x = 0; x < difference+remainder;x++){
       // printf("%s\n","Should all be done joining" );
        pthread_join(thread[x],NULL);
    }

    
    
    boolean5 = 0;

    pthread_join(redThread,NULL);
    pthread_cleanup_push(printer,NULL);
    
    pthread_cleanup_pop(1);
   


    return 0;
}



static void* map1(void* v){
     pthread_mutex_lock(&test);

    //create new dataCol Struct
    dataCol5 * newDum = malloc(sizeof(dataCol5));
    newDum= (dataCol5*)v;

    //OPEN FILE

    FILE* fp;
    fp = fopen(newDum->path, "r+");

    //char *final;
    //CREATE VARIABLES NEEDED with enough space
    //Time stamp
    char timeStamp[500];
    char ip[500];
    char duration[500];
    char country[500];

    //Varabies for part a/B
    //Duration for each website 
    double sumDuration = 0;
    double counter = 0;
    double avgDuration = 0;
    int year;
    int check;

    //Variabes for C/D
    double sumOfUsers=0;
    double avgUsers=0;
    double numYears=0;
    //We gotta lock this up or else threads will corrupt data 
    

    while((check = (fscanf(fp, "%[^,],%[^,],%[^,],%s\n", timeStamp, ip, duration,country))!=EOF)){
        


        year = whatYear5(timeStamp);
        
        int durTime = atoi(duration);
        
        //Part a/b      
        sumDuration += durTime;   
        counter ++;   

        //Count the years
        int existsY = yearExist5(year);
        //It exists 
        if(existsY == 1){
            sumOfUsers++;
        }
        //create a linked list for it
        else{
            numYears++;
            years5 * dumYear = malloc(sizeof(years5));
            dumYear ->next = NULL;
            dumYear ->year1 = year;
            if(yearsHead5==NULL){
                yearsHead5 = dumYear;
                curYears5= yearsHead5;
            }
            else{
                curYears5->next = dumYear;
                curYears5 = dumYear;
            }
            sumOfUsers++;
        }

        //Check if the country code exists
        int existsC = codeExist5(country);
        if(existsC == 1){
            places5 *dumPlaces = placesHead5;
            while(dumPlaces!= NULL){
                if((strcmp(country,dumPlaces->place)==0)){
                     dumPlaces ->counter+=1;   
                }
                dumPlaces = dumPlaces -> next;
                }
            }
        
        else{
           // printf("%s\n", "we here?");
            places5 * dumPlaces = malloc(sizeof(places5));
            dumPlaces->counter = 1;
            dumPlaces -> next = NULL;
            dumPlaces->place = strdup(country);

            if(placesHead5==NULL){
                placesHead5 = dumPlaces;
                curPlaces5 = placesHead5;
            }
            else{
                curPlaces5->next = dumPlaces;
                curPlaces5 = dumPlaces;

            }

        }


        //Reset the variables
        memset(&timeStamp[0],0,sizeof(timeStamp));
        memset(&ip[0], 0, sizeof(ip));    
        memset(&country[0], 0, sizeof(country));
        memset(&duration[0],0,sizeof(duration));
    }

   

    avgUsers = sumOfUsers/numYears;

    newDum -> maxCountry = strdup(findLargestCountry5());
    newDum -> countryCount = findLargestCountryInt5();

    placesHead5=NULL;
    yearsHead5=NULL;

    
    //Put the year in
    newDum->year = year;

    //Gets the avg Duration of each file
    avgDuration = sumDuration/counter;
    newDum->avgDur = avgDuration;


    //Set next as null
    newDum->next= NULL;

    //Set the avgUser
    newDum->avgUser =avgUsers;

    //Put locks Here JUST INCASE something goes wrong 
    //Rare cases it might
    
    //Check if this is the head;
    /*
    if(headCol5 == NULL){
        
        headCol5 = newDum;
        curCol5 = headCol5;
    //If it's not you do this
    }else{
        curCol5->next = newDum;
        curCol5 = newDum;

    }*/
    


    //Clear the years Struct
    

    
    //pthread_mutex_lock(&fileLock);
    //printf("%s\n", "are we stuck");
    int fd[2];
    socketpair(PF_LOCAL, SOCK_STREAM, 0, fd);
 
    char * sockPath = strdup(newDum->path);
    write(0, sockPath, sizeof(sockPath));

   
    printf("%s\n", sockPath);
    
    fflush(stdout);
     //printf("%s\n", "we doing");
    //char * testing = strdup("sadf\n");
    //write(0,testing,sizeof(testing));
    //pthread_mutex_unlock(&fileLock);


    fclose(fp);
     pthread_mutex_unlock(&test);
    return headCol5;
}



static void* map(void* v){
    
   
    illCount * newNew = malloc(sizeof(illCount));
    newNew = (illCount*)v;

    int dummy;
    int counterOuterLoop = newNew->counter;
    //printf("Tihs is ther counter outlerLoop%d\n", counterOuterLoop);
    //printf("Tihs should be four no? %d\n", counterOuterLoop);
    for(dummy = 0; dummy < counterOuterLoop; dummy++){
        pthread_mutex_lock(&headProtection);
        
        //printf("%s\n", "got tohere");

        
        file1 = readdir(dir);
       
        if((strcmp(file1->d_name,".")==0) || (strcmp(file1->d_name,"..")==0)){
            
            dummy--;
        }
        else{

            char *path1 = malloc(strlen(DATA_DIR)+strlen(file1->d_name)+5);
            strcpy(path1,DATA_DIR);
            strcat(path1,"/");
            strcat(path1,file1->d_name);
       
            newCol = malloc(sizeof(dataCol5));
            newCol->path = strdup(path1);
            map1(newCol);

             
    }
        pthread_mutex_unlock(&headProtection);
    }

    return NULL;
}


static void* reduce(void* v){

  while(1){
   // printf("%s\n","the fuck" );
   //char buf[5000];
  //pthread_mutex_lock(&fileLock);
  
  //read(1, buf, sizeof(buf));
  //printf("%s\n", buf);
 // printf("parent received '%.*s'\n", x, buf);
  //pthread_mutex_unlock(&fileLock);

}
   // epoll_create();
    
    return NULL;
}
