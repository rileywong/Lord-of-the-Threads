#include "lott.h"
#include "extra3.h"
#include <sys/file.h>
#include <pthread.h>

static void* map(void*);
static void* reduce(void*);



DIR *dir;
struct dirent *file1;
pthread_mutex_t test;
pthread_mutex_t fileLock;
FILE *temp;
FILE* tempRead;
sem_t writeLock;
sem_t readLock;

double maxUserRed;
double minUserRed3 = 99999999;

double maxDurRed;
double minDurRed3 = 99999999;

int countryCount;

char * ccRedPath;

char * maxUpathRed;
char * minUpathRed;

char * maxDpathRed;
char * minDpathRed;

int boolean3 =1;


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
        printf("Result : %f, %s\n",minDurRed3,minDpathRed ); 
    }
    if((strcmp(QUERY_STRINGS[current_query],"C") ==0)){
        //printf("%s\n", "This is C" );
        printf("Result : %f, %s\n",maxUserRed,maxUpathRed ); 
    }
    if((strcmp(QUERY_STRINGS[current_query],"D") ==0)){
        //printf("%s\n", "This is D" );
        printf("Result : %f, %s\n",minUserRed3,minUpathRed ); 
    }
    if((strcmp(QUERY_STRINGS[current_query],"E") ==0)){
        //printf("%s\n", "This is E" );
         printf("Result : %d, %s\n",findLargestCountryIntRed3(),ccRedPath ); 
    }

}

int part3(size_t nthreads) {
    if(nthreads == 0){
        printf("%s\n", "Nthreads cant be 0");
        return 0;
    }

    // initialize all variables so that everything works smoothly. 
    maxUpathRed = strdup("ZZZZ");
    minUpathRed = strdup("ZZZZ");

    maxDpathRed = strdup("ZZZZ");
    minDpathRed = strdup("ZZZZ");

    minUserRed3 = 999999999;
    minDurRed3 = 999999999;
   
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


    


    int totalFiles = nfiles3(DATA_DIR);
    pthread_t thread[totalFiles];

    int divider = 0;
    int remainder = 0;
    int difference = 0;

    //Algorithm to split work evenly

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

     illCount * counter3 = malloc(sizeof(illCount));
     counter3->counter = divider;

   // printf("%s\n","here?" );

     for(outerLoop = 0 ; outerLoop < remainder; outerLoop++){
      

        pthread_create(&thread[outerLoop],NULL, map,counter1);  
         char name[50];
            sprintf(name, "map %d",outerLoop);
            pthread_setname_np(thread[outerLoop],name);
       
     }

         
    for(lastLoop = 0; lastLoop < difference; lastLoop++){ 
       
        pthread_create(&thread[lastLoop+remainder],NULL, map,counter3);  
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

    boolean3 = 0;

    pthread_join(redThread,NULL);
    pthread_cleanup_push(printer,NULL);
    
    pthread_cleanup_pop(1);
    unlink("mapred.tmp");


    return 0;
}



static void* map1(void* v){
     pthread_mutex_lock(&test);

    //create new dataCol Struct
    dataCol3 * newDum = malloc(sizeof(dataCol3));
    newDum= (dataCol3*)v;

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
        
        year = whatYear3(timeStamp);
        
        int durTime = atoi(duration);
        
        //Part a/b      
        sumDuration += durTime;   
        counter ++;   

        //Count the years
        int existsY = yearExist3(year);
        //It exists 
        if(existsY == 1){
            sumOfUsers++;
        }
        //create a linked list for it
        else{
            numYears++;
            years3 * dumYear = malloc(sizeof(years3));
            dumYear ->next = NULL;
            dumYear ->year1 = year;
            if(yearsHead3==NULL){
                yearsHead3 = dumYear;
                curYears3= yearsHead3;
            }
            else{
                curYears3->next = dumYear;
                curYears3 = dumYear;
            }
            sumOfUsers++;
        }

        //Check if the country code exists
        int existsC = codeExist3(country);
        if(existsC == 1){
            places3 *dumPlaces = placesHead3;
            while(dumPlaces!= NULL){
                if((strcmp(country,dumPlaces->place)==0)){
                     dumPlaces ->counter+=1;   
                }
                dumPlaces = dumPlaces -> next;
                }
            }
        
        else{
           // printf("%s\n", "we here?");
            places3 * dumPlaces = malloc(sizeof(places3));
            dumPlaces->counter = 1;
            dumPlaces -> next = NULL;
            dumPlaces->place = strdup(country);

            if(placesHead3==NULL){
                placesHead3 = dumPlaces;
                curPlaces3 = placesHead3;
            }
            else{
                curPlaces3->next = dumPlaces;
                curPlaces3 = dumPlaces;

            }

        }


        //Reset the variables
        memset(&timeStamp[0],0,sizeof(timeStamp));
        memset(&ip[0], 0, sizeof(ip));    
        memset(&country[0], 0, sizeof(country));
        memset(&duration[0],0,sizeof(duration));
    }

   

    avgUsers = sumOfUsers/numYears;

    newDum -> maxCountry = strdup(findLargestCountry3());
    newDum -> countryCount = findLargestCountryInt3();

    placesHead3=NULL;
    yearsHead3=NULL;

    
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
    if(headCol3 == NULL){
        
        headCol3 = newDum;
        curCol3 = headCol3;
    //If it's not you do this
    }else{
        curCol3->next = newDum;
        curCol3 = newDum;

    }
    


    //Clear the years Struct
    temp = fopen("mapred.tmp","ab+");

    flock(fileno(temp), LOCK_EX);
    //pthread_mutex_lock(&fileLock);
    fprintf(temp, "%s,%f,%f,%s,%d\n",newDum->path,newDum->avgUser,newDum->avgDur, newDum->maxCountry,newDum->countryCount );
    fclose(temp);
    flock(fileno(temp), LOCK_UN);
    //pthread_mutex_lock(&fileLock);


    fclose(fp);
     pthread_mutex_unlock(&test);
    return headCol3;
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

            char *path1 = malloc(strlen(DATA_DIR)+strlen(file1->d_name)+3);
            strcpy(path1,DATA_DIR);
            strcat(path1,"/");
            strcat(path1,file1->d_name);
       
            newCol = malloc(sizeof(dataCol3));
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


        char path[500];
        double avgUser= 0;
        double avgDur = 0;
        char  maxCountry[5];
        int countryCount=0;

        tempRead = fopen("mapred.tmp","ab+");
        if(tempRead== NULL){
            printf("%s\n","File Open failed?" );
        }
        
        //printf("%s\n","is this the error" );
        pthread_setcancelstate(PTHREAD_CANCEL_DISABLE,NULL);
        while(boolean3){
            fscanf(tempRead, "%[^,],%[^,],%[^,],%[^,],%s\n", path, avgUser1, avgDur1,maxCountry,countryCount1);
        
         pthread_mutex_lock(&headProtection);
        //flock(fileno(temp), LOCK_EX);
        //pthread_mutex_lock(&fileLock);
        if((double)atof(avgUser1)!=0){
        avgUser = (double)atof(avgUser1);
        
           
        avgDur = (double)atof(avgDur1);
        countryCount = atoi(countryCount1);


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

        if(minUserRed3>=avgUser && avgUser!=0){
            if(strcmp(maxUpathRed,"ZZZZ") == 0){
                minUserRed3 = avgUser;
                minUpathRed = strdup(path+5);
            }
            else 
            if(minUserRed3 == avgUser){
                if(strcmp(path +5,minUpathRed)<0){
                    //printf("%s\n", "Names should change");
                    minUpathRed = strdup(path+5);
                }
            }else{
                minUserRed3 = avgUser;
                minUpathRed = strdup(path+5);
            }
            
        }

        // Calculating max avgDur   
            if(avgDur>maxDurRed){
                maxDurRed = avgDur;
                maxDpathRed = strdup(path +5);
            }
        // Calculating min Dur
            if(avgDur<minDurRed3 && avgUser !=0){
                minDurRed3 = avgDur;
                minDpathRed = strdup(path +5);
            }
        
        // Calculating the country with most users
            int existsC = codeExistRed3(maxCountry);
            
            //printf("%s\n", maxCountry);
            if(existsC == 1){
                //printf("This country exists%s\n", maxCountry);

                placesRed3 *dumPlaces = placesHeadRed3;
                while(dumPlaces != NULL){
                    if((strcmp(maxCountry,dumPlaces->place)==0)){
                       
                        
                        dumPlaces ->counter = dumPlaces -> counter +countryCount ;   
                       
                    } 
                    dumPlaces= dumPlaces->next;
                    }         
                }
        
            else{
            //printf("%s\n", "we here?");
            placesRed3 * dumPlaces3 = malloc(sizeof(placesRed3));
            dumPlaces3->counter = countryCount;
            dumPlaces3 -> next = NULL;
            dumPlaces3->place = strdup(maxCountry);



            if(placesHeadRed3==NULL){
                placesHeadRed3 = dumPlaces3;
                curPlacesRed3 = placesHeadRed3;
            }
            else{
                curPlacesRed3->next = dumPlaces3;
                curPlacesRed3 = dumPlaces3;
            }
        


       }

       //countryCount = findLargestCountryIntRed3();
       //printf("This is the countr ctsfd%d\n",countryCount );
       ccRedPath = strdup(findLargestCountryRed3());
        
        /*
        printf("%s\n",path);
        printf("%f\n", avgUser);
        printf("%f\n", avgDur);
        printf("%s\n", maxCountry);
        printf("%d\n", countryCount);
        */
        }
        memset(&path[0],0,sizeof(path));
        memset(&avgUser1[0],0,sizeof(avgUser1));
        memset(&avgDur1[0],0,sizeof(avgDur1));
        memset(&maxCountry[0],0,sizeof(maxCountry));
        memset(&countryCount1[0],0,sizeof(countryCount1));
        pthread_mutex_unlock(&headProtection);
        //flock(fileno(temp), LOCK_UN);
        
        //pthread_mutex_lock(&fileLock);
/*
        printf("Max Users %f\n", maxUserRed);
    printf("Max User Path %s\n", maxUpathRed);
    printf("Min Users %f\n", minUserRed3);
    printf("Min User Path %s\n", minUpathRed);
    printf("Max avg %f\n", maxDurRed);
    printf("Max avg Path %s\n", maxDpathRed);
    printf("Min avg %f\n", minDurRed3);
    printf("Min avg Path %s\n", minDpathRed);
    printf("%d\n", countryCount);
    */

/*
}
else{
   // sem_post(&writeLock);
    //sem_wait(&readLock);
}
*/      
    }

    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE,NULL);
    //printf("Last timeThis is the countr ctsfd%d\n",countryCount );
    /*
    printf("Max Users %f\n", maxUserRed);
    printf("Max User Path %s\n", maxUpathRed);
    printf("Min Users %f\n", minUserRed3);
    printf("Min User Path %s\n", minUpathRed);
    printf("Max avg %f\n", maxDurRed);
    printf("Max avg Path %s\n", maxDpathRed);
    printf("Min avg %f\n", minDurRed3);
    printf("Min avg Path %s\n", minDpathRed);
    printf("%d\n", countryCount);
    */
   // printf("%s\n", ccRedPath);
    
    return NULL;
}
