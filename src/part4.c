#include "lott.h"
#include "extra4.h"
#include <sys/file.h>
#include <pthread.h>

static void* map(void*);
static void* reduce(void*);



DIR *dir;
struct dirent *file1;
pthread_mutex_t test;
pthread_mutex_t fileLock;

sem_t writeLock;
sem_t readLock;

double maxUserRed;
double minUserRed4 = 99999999;

double maxDurRed;
double minDurRed4 = 99999999;

int countryCount;

char * ccRedPath;

char * maxUpathRed;
char * minUpathRed;

char * maxDpathRed;
char * minDpathRed;

int boolean4 =1;


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
        printf("Result : %f, %s\n",maxDurRed,maxDpathRed ); 
    }
    if((strcmp(QUERY_STRINGS[current_query],"B") ==0)){
        //printf("%s\n", "This is B" );
        printf("Result : %f, %s\n",minDurRed4,minDpathRed ); 
    }
    if((strcmp(QUERY_STRINGS[current_query],"C") ==0)){
        //printf("%s\n", "This is C" );
        printf("Result : %f, %s\n",maxUserRed,maxUpathRed ); 
    }
    if((strcmp(QUERY_STRINGS[current_query],"D") ==0)){
        //printf("%s\n", "This is D" );
        printf("Result : %f, %s\n",minUserRed4,minUpathRed ); 
    }
    if((strcmp(QUERY_STRINGS[current_query],"E") ==0)){
        //printf("%s\n", "This is E" );
         printf("Result : %d, %s\n",findLargestCountryIntRed4(),ccRedPath ); 
    }

}

int part4(size_t nthreads) {
    if(nthreads == 0){
        printf("%s\n", "Nthreads cant be 0");
        return 0;
    }

    maxUpathRed = strdup("ZZZZ");
    minUpathRed = strdup("ZZZZ");

    maxDpathRed = strdup("ZZZZ");
    minDpathRed = strdup("ZZZZ");

    minUserRed4 = 999999999;
    minDurRed4 = 999999999;
   
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


    


    int totalFiles = nfiles4(DATA_DIR);
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

     illCount * counter4 = malloc(sizeof(illCount));
     counter4->counter = divider;

   // printf("%s\n","here?" );

     for(outerLoop = 0 ; outerLoop < remainder; outerLoop++){
      

        pthread_create(&thread[outerLoop],NULL, map,counter1);  
         char name[50];
            sprintf(name, "map %d",outerLoop);
            pthread_setname_np(thread[outerLoop],name);
       
     }

         
    for(lastLoop = 0; lastLoop < difference; lastLoop++){ 
       
        pthread_create(&thread[lastLoop+remainder],NULL, map,counter4);  
         char name[50];
            sprintf(name, "map %d",lastLoop+remainder);
            pthread_setname_np(thread[lastLoop+remainder],name);
     }
     
       pthread_t redThread;
    pthread_create(&redThread,NULL,reduce,NULL);
     char name[50];
            sprintf(name, "reduce");
            pthread_setname_np(redThread,name);


    int x;
    for(x = 0; x < difference+remainder;x++){
       // printf("%s\n","Should all be done joining" );
        pthread_join(thread[x],NULL);
    }

   
    int sleeper = 0;
    // We need to do this for each file because unlink will unlink
    // file before all operations are done, so i created a delay
    for(sleeper= 0; sleeper<totalFiles;sleeper++){
   
    usleep(250000);
   
    }

    boolean4 = 0;

    pthread_cancel(redThread);
    pthread_cleanup_push(printer,NULL);
    
    pthread_cleanup_pop(1);
    
    //printList4();


    return 0;
}



static void* map1(void* v){
     pthread_mutex_lock(&test);

    //create new dataCol Struct
    dataCol4 * newDum = malloc(sizeof(dataCol4));
    newDum= (dataCol4*)v;

    //OPEN FILE

    FILE* fp;
    fp = fopen(newDum->path, "r+");


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
        
        year = whatYear4(timeStamp);
        
        int durTime = atoi(duration);
        
        //Part a/b      
        sumDuration += durTime;   
        counter ++;   

        //Count the years
        int existsY = yearExist4(year);
        //It exists 
        if(existsY == 1){
            sumOfUsers++;
        }
        //create a linked list for it
        else{
            numYears++;
            years4 * dumYear = malloc(sizeof(years4));
            dumYear ->next = NULL;
            dumYear ->year1 = year;
            if(yearsHead4==NULL){
                yearsHead4 = dumYear;
                curYears4= yearsHead4;
            }
            else{
                curYears4->next = dumYear;
                curYears4 = dumYear;
            }
            sumOfUsers++;
        }

        //Check if the country code exists
        int existsC = codeExist4(country);
        if(existsC == 1){
            places4 *dumPlaces = placesHead4;
            while(dumPlaces!= NULL){
                if((strcmp(country,dumPlaces->place)==0)){
                     dumPlaces ->counter+=1;   
                }
                dumPlaces = dumPlaces -> next;
                }
            }
        
        else{
           // printf("%s\n", "we here?");
            places4 * dumPlaces = malloc(sizeof(places4));
            dumPlaces->counter = 1;
            dumPlaces -> next = NULL;
            dumPlaces->place = strdup(country);

            if(placesHead4==NULL){
                placesHead4 = dumPlaces;
                curPlaces4 = placesHead4;
            }
            else{
                curPlaces4->next = dumPlaces;
                curPlaces4 = dumPlaces;

            }

        }


        //Reset the variables
        memset(&timeStamp[0],0,sizeof(timeStamp));
        memset(&ip[0], 0, sizeof(ip));    
        memset(&country[0], 0, sizeof(country));
        memset(&duration[0],0,sizeof(duration));
    }

   

    avgUsers = sumOfUsers/numYears;

    newDum -> maxCountry = strdup(findLargestCountry4());
    newDum -> countryCount = findLargestCountryInt4();

    placesHead4=NULL;
    yearsHead4=NULL;

    
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
    sem_wait(&writeLock);

    if(headCol4 == NULL){
       
        headCol4 = newDum;
        curCol4 = headCol4;
    //If it's not you do this
    }else{

        curCol4->next = newDum;
        curCol4 = newDum;

    }

    sem_post(&readLock);
    


  

    fclose(fp);
     pthread_mutex_unlock(&test);
    return headCol4;
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

            char *path1 = malloc(strlen(DATA_DIR)+strlen(file1->d_name)+4);
            strcpy(path1,DATA_DIR);
            strcat(path1,"/");
            strcat(path1,file1->d_name);
       
            newCol = malloc(sizeof(dataCol4));
            newCol->path = strdup(path1);
            map1(newCol);

             
    }
        pthread_mutex_unlock(&headProtection);
    }

    return NULL;
}


static void* reduce(void* v){
  
        char avgUser1[500];
        char avgDur1[500];
        char countryCount1[500];


        char *path;
        double avgUser= 0;
        double avgDur = 0;
        char  *maxCountry;
        int countryCount=0;
        sem_wait(&readLock);
        
        dataCol4 * dum =malloc(sizeof(dataCol4));
        dum = headCol4;
        sem_post(&readLock);
        //printf("%s\n","is this the error" );
       // pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);


        while(boolean4){
            sem_wait(&readLock);
            
        path = strdup(dum->path);

        maxCountry = strdup(dum->maxCountry);

        avgUser = dum->avgUser;
 
        avgDur = dum->avgDur;

        countryCount = dum->countryCount;
        
        
         
        //flock(fileno(temp), LOCK_EX);
        //pthread_mutex_lock(&fileLock);
        if(avgUser!=0){
        


        //Calculating the maxUser  
        //printf("This is max User %f\n", maxUserRed);  
        //printf("This is average User %f\n", avgUser);
        if(maxUserRed<=avgUser){
            if(strcmp(maxUpathRed,"ZZZZ") == 0){
                maxUserRed = avgUser;
                maxUpathRed = strdup(path+5);
            }
            else 
            if(maxUserRed == avgUser){
                if(strcmp(path +5,maxUpathRed)<0){
                    //printf("%s\n", "Names should change");
                    maxUpathRed = strdup(path+5);
                }
            }else{
                maxUserRed = avgUser;
                maxUpathRed = strdup(path+5);
            }
            
        }

        //Calculating the minUser

        if(minUserRed4>=avgUser && avgUser!=0){
            if(strcmp(maxUpathRed,"ZZZZ") == 0){
                minUserRed4 = avgUser;
                minUpathRed = strdup(path+5);
            }
            else 
            if(minUserRed4 == avgUser){
                if(strcmp(path +5,minUpathRed)<0){
                    //printf("%s\n", "Names should change");
                    minUpathRed = strdup(path+5);
                }
            }else{
                minUserRed4 = avgUser;
                minUpathRed = strdup(path+5);
            }
            
        }

        // Calculating max avgDur   
            if(avgDur>maxDurRed){
                maxDurRed = avgDur;
                maxDpathRed = strdup(path +5);
            }
        // Calculating min Dur
            if(avgDur<minDurRed4 && avgUser !=0){
                minDurRed4 = avgDur;
                minDpathRed = strdup(path +5);
            }
        
        // Calculating the country with most users
            int existsC = codeExistRed4(maxCountry);
            
            //printf("%s\n", maxCountry);
            if(existsC == 1){
                //printf("This country exists%s\n", maxCountry);

                placesRed4 *dumPlaces = placesHeadRed4;
                while(dumPlaces != NULL){
                    if((strcmp(maxCountry,dumPlaces->place)==0)){
                       
                        
                        dumPlaces ->counter = dumPlaces -> counter +countryCount ;   
                       
                    } 
                    dumPlaces= dumPlaces->next;
                    }         
                }
        
            else{
            //printf("%s\n", "we here?");
            placesRed4 * dumPlaces4 = malloc(sizeof(placesRed4));
            dumPlaces4->counter = countryCount;
            dumPlaces4 -> next = NULL;
            dumPlaces4->place = strdup(maxCountry);



            if(placesHeadRed4==NULL){
                placesHeadRed4 = dumPlaces4;
                curPlacesRed4 = placesHeadRed4;
            }
            else{
                curPlacesRed4->next = dumPlaces4;
                curPlacesRed4 = dumPlaces4;
            }
        


       }

       //countryCount = findLargestCountryIntRed4();
       //printf("This is the countr ctsfd%d\n",countryCount );
       ccRedPath = strdup(findLargestCountryRed4());
        
        /*
        printf("%s\n",path);
        printf("%f\n", avgUser);
        printf("%f\n", avgDur);
        printf("%s\n", maxCountry);
        printf("%d\n", countryCount);
        */
        
        }
        
        memset(&avgUser1[0],0,sizeof(avgUser1));
        memset(&avgDur1[0],0,sizeof(avgDur1));
        
        memset(&countryCount1[0],0,sizeof(countryCount1));
       
     sem_post(&writeLock);
        sem_wait(&readLock);
        dum = dum->next;
        sem_post(&readLock);
    }

 
    return NULL;
}
