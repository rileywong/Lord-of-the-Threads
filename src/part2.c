#include "lott.h"
#include "extra2.h"

static void* map(void*);
static void* reduce(void*);



DIR *dir;
struct dirent *file1;
pthread_mutex_t test;

struct illCount{
    int counter;
}typedef illCount;

int part2(size_t nthreads) {

    if(nthreads == 0){
        printf("%s\n", "Nthreads cant be 0");
        return 0;
    }
    pthread_mutex_init(&test,NULL);
    pthread_mutex_init(&headProtection,NULL);

    

    if((dir = opendir(DATA_DIR))==NULL){
        //printf("Could not open : %s\n",dir );
        printf("%s\n","file cant be found" );
        return -1;
    }

    

    int totalFiles = nfiles2(DATA_DIR);

    pthread_t thread[totalFiles];
    //pthread_t threadReduce[totalFiles];
   // int counter = 0;

    int divider = 0;
    int remainder = 0;
    int difference = 0;

    //Algorithm to split the nthreads work evenly amongs the threads 
    divider = totalFiles/nthreads;
    remainder = totalFiles - (divider * nthreads);

    if(totalFiles < nthreads){
    difference = totalFiles -remainder;
    }
    else{
        difference = nthreads -remainder;
    }
    
    /*
    printf("This is the divider : %d\n", divider );
    printf("This is the remainder: %d\n", remainder);
    printf("This is the difference : %d\n", difference);
*/


    int outerLoop;

    int x;
 



/*
    for(x = 0; x < remainder;x++){
        pthread_join(thread[x],NULL);
    }*/


     int lastLoop = 0;
     
     // Create two structs to control how much work threads will do
     illCount * counter1 = malloc(sizeof(illCount));
     counter1->counter = divider + 1;

     illCount * counter2 = malloc(sizeof(illCount));
     counter2->counter = divider;

   
     //Loop to do divider + 1  * remainder work
     for(outerLoop = 0 ; outerLoop < remainder; outerLoop++){
      
        pthread_create(&thread[outerLoop],NULL, map,counter1);  
        
        char name[50];
            sprintf(name, "map %d",outerLoop);
            pthread_setname_np(thread[outerLoop],name);
            
       
     }
    

     //Loop to do divider * difference work
    for(lastLoop = 0; lastLoop < difference; lastLoop++){ 
       
        pthread_create(&thread[lastLoop+remainder],NULL, map,counter2); 
        char name[50];
            sprintf(name, "map %d",lastLoop+remainder);
            //printf("%s\n", name);
            pthread_setname_np(thread[lastLoop+remainder],name);
             
     }
     
     /*
     for(x = 0; x < difference;x++){
        pthread_join(thread[x],NULL);
    }*/
    
    

     
        //Join all threads
    for(x = 0; x < difference+remainder;x++){
      
        pthread_join(thread[x],NULL);
    }
    
    
    

    //printList2();

    printf(
        "Part: %s\n"
        "Query: %s\n",

        PART_STRINGS[current_part], QUERY_STRINGS[current_query]);

    reduce(newCol);
    //pthread_create(&thread[counter],NULL,reduce,newCol);

    return 0;
}



static void* map1(void* v){
     pthread_mutex_lock(&test);

    //create new dataCol Struct
    dataCol2 * newDum = malloc(sizeof(dataCol2));
    newDum= (dataCol2*)v;

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
        
        year = whatYear2(timeStamp);
        
        int durTime = atoi(duration);
        
        //Part a/b      
        sumDuration += durTime;   
        counter ++;   

        //Count the years
        int existsY = yearExist2(year);
        //It exists 
        if(existsY == 1){
            sumOfUsers++;
        }
        //create a linked list for it
        else{
            numYears++;
            years2 * dumYear = malloc(sizeof(years2));
            dumYear ->next = NULL;
            dumYear ->year1 = year;
            if(yearsHead2==NULL){
                yearsHead2 = dumYear;
                curYears2= yearsHead2;
            }
            else{
                curYears2->next = dumYear;
                curYears2 = dumYear;
            }
            sumOfUsers++;
        }

        //Check if the country code exists
        int existsC = codeExist2(country);
        if(existsC == 1){
            places2 *dumPlaces = placesHead2;
            while(dumPlaces!= NULL){
                if((strcmp(country,dumPlaces->place)==0)){
                     dumPlaces ->counter+=1;   
                }
                dumPlaces = dumPlaces -> next;
                }
            }
        
        else{
           // printf("%s\n", "we here?");
            places2 * dumPlaces = malloc(sizeof(places2));
            dumPlaces->counter = 1;
            dumPlaces -> next = NULL;
            dumPlaces->place = strdup(country);

            if(placesHead2==NULL){
                placesHead2 = dumPlaces;
                curPlaces2 = placesHead2;
            }
            else{
                curPlaces2->next = dumPlaces;
                curPlaces2 = dumPlaces;

            }

        }


        //Reset the variables
        memset(&timeStamp[0],0,sizeof(timeStamp));
        memset(&ip[0], 0, sizeof(ip));    
        memset(&country[0], 0, sizeof(country));
        memset(&duration[0],0,sizeof(duration));
    }

   

    avgUsers = sumOfUsers/numYears;

    newDum -> maxCountry = strdup(findLargestCountry2());
    newDum -> countryCount = findLargestCountryInt2();

    placesHead2=NULL;
    yearsHead2=NULL;

    
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
    if(headCol2 == NULL){
        
        headCol2 = newDum;
        curCol2 = headCol2;
    //If it's not you do this
    }else{
        curCol2->next = newDum;
        curCol2 = newDum;

    }
    


    //Clear the years Struct

    fclose(fp);
     pthread_mutex_unlock(&test);
    return headCol2;
}


static void* map(void* v){
    
    //This map function will call Map1 a certain number of times
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

            char *path1 = malloc(strlen(DATA_DIR)+strlen(file1->d_name)+2);
            strcpy(path1,DATA_DIR);
            strcat(path1,"/");
            strcat(path1,file1->d_name);
       
            newCol = malloc(sizeof(dataCol2));
            newCol->path = strdup(path1);
            map1(newCol);

             
    }
        pthread_mutex_unlock(&headProtection);
    }

    return NULL;
}


static void* reduce(void* v){
      if((strcmp(QUERY_STRINGS[current_query],"A") ==0)){
       // printf("%s\n", "This is A" );
        maxAvgDur2();
    }
    if((strcmp(QUERY_STRINGS[current_query],"B") ==0)){
        //printf("%s\n", "This is B" );
        minAvgDur2();
    }
    if((strcmp(QUERY_STRINGS[current_query],"C") ==0)){
        //printf("%s\n", "This is C" );
        maxAvgUsers2();
    }
    if((strcmp(QUERY_STRINGS[current_query],"D") ==0)){
        //printf("%s\n", "This is D" );
        minAvgUsers2();
    }
    if((strcmp(QUERY_STRINGS[current_query],"E") ==0)){
        //printf("%s\n", "This is E" );
        mostUsers2();
    }
    return NULL;
}
