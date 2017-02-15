

#include "lott.h"
#include "extra1.h"

static void* map(void*);
static void* reduce(void*);

int part1(){


    //This is so the list is properly created/protected
    pthread_mutex_init(&headProtection,NULL);
    pthread_mutex_init(&work,NULL);

    /* DELETE THIS: YOU DO NOT CALL THSESE DIRECTLY YOU WILL SPAWN THEM AS THREADS */
    //map(NULL);
    //reduce(NULL);
    /* DELETE THIS: THIS IS TO QUIET COMPILER ERRORS */

    DIR *dir;

  
    // Open directory
    if((dir = opendir(DATA_DIR))==NULL){
        //printf("Could not open : %s\n",dir );
        printf("%s\n","file cant be found" );
        return -1;
    }

    struct dirent *file1;

    // Count the total number of files

    int totalFiles = nfiles(DATA_DIR);
    
    printf("Total Files %d\n", totalFiles);
    pthread_t thread[totalFiles];
    int counter = 0;


    //Loop through the entire directroy, create a new thread each time.
    while((file1 = readdir(dir))!=NULL){
        if((strcmp(file1->d_name,".")==0) || (strcmp(file1->d_name,"..")==0)){
            //totalFiles = totalFiles;
        }
        else{

           // FILE *fp;

            char *path1 = malloc(strlen(DATA_DIR)+strlen(file1->d_name)+2);
            strcpy(path1,DATA_DIR);
            strcat(path1,"/");
            strcat(path1,file1->d_name);
            //strcat(path1,"\0");

            
            newCol = malloc(sizeof(dataCol));
            newCol->path = strdup(path1);

                    

            pthread_create(&thread[counter],NULL, map,newCol);
            char name[50];
            sprintf(name, "map %d",counter);
           // printf("%s\n", name);
            pthread_setname_np(thread[counter],name);
            
            counter++;
        }
    }

   

    

    //printf("Total files = %i\n", totalFiles);


    
    int x;
    
    //Make sure all threads are joined 

    for(x = 0; x < totalFiles;x++){
        pthread_join(thread[x],NULL);
    }
    //Destroy Mutex for listLock
    pthread_mutex_destroy(&headProtection);

    //Print the entire List this is just to check if everyting is correct
    //printList();

    printf(
        "Part: %s\n"
        "Query: %s\n",
        PART_STRINGS[current_part], QUERY_STRINGS[current_query]);


    reduce(newCol);

    
    return 0;
}





static void* map(void* v){



    //create new dataCol Struct
    dataCol * newDum = malloc(sizeof(dataCol));
    newDum= (dataCol*)v;

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
    pthread_mutex_lock(&headProtection);


    //Loop through entire file until end of file
    // Update the linked list every time
    while((check = (fscanf(fp, "%[^,],%[^,],%[^,],%s\n", timeStamp, ip, duration,country))!=EOF)){
        
        year = whatYear(timeStamp);
        
        int durTime = atoi(duration);
        
        //Part a/b      
        sumDuration += durTime;   
        counter ++;   

        //Count the years
        int existsY = yearExist(year);
        //It exists 
        if(existsY == 1){
            sumOfUsers++;
        }
        //create a linked list for it
        else{
            numYears++;
            years * dumYear = malloc(sizeof(years));
            dumYear ->next = NULL;
            dumYear ->year1 = year;
            if(yearsHead==NULL){
                yearsHead = dumYear;
                curYears= yearsHead;
            }
            else{
                curYears->next = dumYear;
                curYears = dumYear;
            }
            sumOfUsers++;
        }

        pthread_mutex_lock(&work);

        //Check if the country code exists
        int existsC = codeExist(country);
        if(existsC == 1){
            places *dumPlaces = placesHead;
            while(dumPlaces!= NULL){
                if((strcmp(country,dumPlaces->place)==0)){
                     dumPlaces ->counter += 1;   
                }
                dumPlaces = dumPlaces -> next;
                }
            }
        
        else{
           // printf("%s\n", "we here?");
            places * dumPlaces = malloc(sizeof(places));
            dumPlaces->counter = 1;
            dumPlaces -> next = NULL;
            dumPlaces->place = strdup(country);

            if(placesHead==NULL){
                placesHead = dumPlaces;
                curPlaces = placesHead;
            }
            else{
                curPlaces->next = dumPlaces;
                curPlaces = dumPlaces;

            }

        }
        pthread_mutex_unlock(&work);


        //Reset the variables
        memset(&timeStamp[0],0,sizeof(timeStamp));
        memset(&ip[0], 0, sizeof(ip));    
        memset(&country[0], 0, sizeof(country));
        memset(&duration[0],0,sizeof(duration));
    }

   

    avgUsers = sumOfUsers/numYears;

    newDum -> maxCountry = strdup(findLargestCountry());
    newDum -> countryCount = findLargestCountryInt();

    placesHead=NULL;
    yearsHead=NULL;

    
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
    if(headCol == NULL){
        
        headCol = newDum;
        curCol = headCol;
    //If it's not you do this
    }else{
        curCol->next = newDum;
        curCol = newDum;

    }
    pthread_mutex_unlock(&headProtection);


    //Clear the years Struct

    fclose(fp);
    return NULL;
}

//Reduce all the calculations are done in header and prints are done there too
static void* reduce(void* v){
    if((strcmp(QUERY_STRINGS[current_query],"A") ==0)){
        //printf("%s\n", "This is A" );
        maxAvgDur();
    }
    if((strcmp(QUERY_STRINGS[current_query],"B") ==0)){
        //printf("%s\n", "This is B" );
        minAvgDur();
    }
    if((strcmp(QUERY_STRINGS[current_query],"C") ==0)){
        //printf("%s\n", "This is C" );
        maxAvgUsers();
    }
    if((strcmp(QUERY_STRINGS[current_query],"D") ==0)){
        //printf("%s\n", "This is D" );
        minAvgUsers();
    }
    if((strcmp(QUERY_STRINGS[current_query],"E") ==0)){
        //printf("%s\n", "This is E" );
        mostUsers();
    }
    return NULL;
}