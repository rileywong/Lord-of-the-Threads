
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <semaphore.h>

//Synchronize this 
pthread_mutex_t headProtection;
    

struct dataCol2{
    double avgDur;
    //The year is useless take this out if you have time
    int year;
    char * path;
    double avgUser;
    char * maxCountry;
    int countryCount;
    struct dataCol2 * next;
};

struct years2{
	int year1;
	struct years2 * next;
};

struct places2{
	char * place;
	int counter;
	struct places2 * next;
};


typedef struct places2 places2;
typedef struct years2 years2;
typedef struct dataCol2 dataCol2;

struct dataCol2 * headCol2 = NULL;
struct dataCol2 * curCol2 = NULL;
struct dataCol2 * newCol;

struct places2 * placesHead2 = NULL;
struct places2 * curPlaces2 = NULL;

struct years2 * yearsHead2 = NULL;
struct years2 * curYears2 = NULL;

int whatYear2(char* timeStamp){
	time_t timeS = atoi(timeStamp);
        //printf("This is the timeS%lu\n", timeS);
       	struct tm *tmp;
        tmp = localtime(&timeS);
       // printf("This is the year : %d\n",tmp->tm_year +1900 );
        return tmp->tm_year +1900;
}



int nfiles2(char* dir){
    int totalFiles = 0;
    DIR *directory;

    // CHECK IF DIRECTORY IS LEGITIMATE
    if((directory = opendir(dir))==NULL){
        //printf("Could not open : %s\n",dir );
        return -1;
    }

    // CREATE DIRECT STRUCT
    struct dirent *file1;

    //LOOP THROUGH DIRECTORY INCREMENTING COUNTER EACH TIME UNTIL THE END
    while((file1 = readdir(directory))!=NULL){
        if((strcmp(file1->d_name,".")==0) || (strcmp(file1->d_name,"..")==0)){
            //totalFiles = totalFiles;
        }
        else{
            totalFiles++;
        }
    }

    // CLOSE DIRECTORY
    closedir(directory);
    // RETURN TOTAL NUMBER OF FILES
    return totalFiles;

}

// Use this to check if the list is working or not

void printList2(){
	dataCol2 * dum =headCol2;

	printf("Head address %s\n", headCol2->path);

	while(dum!=NULL){
		printf("This is the path%s\n", dum->path);
		printf("Avg Dur :%f\n", dum->avgDur);
		printf("Year : %d\n", dum->year);
		printf("Average User Count : %f\n", dum->avgUser);
		printf("Largest Country : %s\n", dum->maxCountry);
		printf("Largest Country User : %d\n", dum->countryCount);
		dum = dum->next;
	}

}

int yearExist2(int year){
	years2 * dumYears = yearsHead2;
	while(dumYears!=NULL){
		if(dumYears->year1 == year){
			//returns 1 for true
			return 1;
		}
		dumYears = dumYears -> next;
	}
	//returns 0 for false
	return 0;
}

int codeExist2(char * code){
	places2 * dumPlaces = placesHead2;
	while(dumPlaces!=NULL){
		if((strcmp(code,dumPlaces->place)==0)){
			return 1;
		}
		dumPlaces = dumPlaces -> next;
	}
	return 0;
	
}

char * findLargestCountry2(){
	places2 * dumPlaces = placesHead2;
	int max = 0;
	//Make a char size 2 cause coutnry code only size 2
	char * website = strdup("ZZZZ");
	while(dumPlaces!=NULL){
		if(dumPlaces->counter>=max){
			if(strcmp(website,"ZZZZ") == 0){
				max = dumPlaces->counter;
				website = strdup(dumPlaces->place);
			}
			else 
			if(dumPlaces->counter == max){

				//printf("%s\n","Theyre Equal" );
				if(strcmp(dumPlaces->place,website)<0){
					//printf("%s\n", "Names should change");
					website = strdup(dumPlaces->place);
				}
			}else{
				max = dumPlaces->counter;
				website = strdup(dumPlaces->place);
			}
			
		}
		dumPlaces = dumPlaces ->next;
	}
	//return null on failure, shouldnt really fail unless something above failed
	
	return website;
}

int findLargestCountryInt2(){
	places2 * dumPlaces = placesHead2;
	int max = 0;
	//Make a char size 2 cause coutnry code only size 2
	//char * largestCount1;
	while(dumPlaces!=NULL){
		if(dumPlaces->counter > max){
			max = dumPlaces->counter;
			//largestCount1= strdup(dumPlaces->place);
		}
		dumPlaces = dumPlaces ->next;
	}
	//return null on failure, shouldnt really fail unless something above failed
	
	return max;
}


int maxAvgDur2(){
	dataCol2 * dumData = headCol2;
	double max = 0;
	char * website;
	while(dumData!=NULL){
		if(dumData->avgDur>max){
			max = dumData->avgDur;
			website = strdup(dumData->path +5);
		}
		dumData = dumData -> next;
	}
	printf("Result : %f, %s\n",max,website );
	return max;

}

int minAvgDur2(){
	dataCol2 * dumData = headCol2;
	double max = 10000;
	char * website;
	while(dumData!=NULL){
		if(dumData->avgDur<max){
			max = dumData->avgDur;
			website = strdup(dumData->path +5);
		}
		dumData = dumData -> next;
	}

	printf("Result : %f, %s\n",max,website );
	return max;

}

int maxAvgUsers2(){
	dataCol2 * dumData = headCol2;
	double max = 0;
	char * website = strdup("ZZZZ");
	while(dumData!=NULL){
		
		if(dumData->avgUser>=max){
			if(strcmp(website,"ZZZZ") == 0){
				max = dumData-> avgUser;
				website = strdup(dumData->path+5);
			}
			else 
			if(dumData->avgUser == max){

				//printf("%s\n","Theyre Equal" );
				if(strcmp(dumData->path +5,website)<0){
					//printf("%s\n", "Names should change");
					website = strdup(dumData->path+5);
				}
			}else{
				max = dumData->avgUser;
				website = strdup(dumData->path+5);
			}
			
		}
		dumData = dumData -> next;
	}
	printf("Result : %f, %s\n",max,website );
	return max;

}

int minAvgUsers2(){
	dataCol2 * dumData = headCol2;
	double max = 10000000;
	char * website=strdup("ZZZZ");
	while(dumData!=NULL){
		if(dumData->avgUser<=max){
			if(strcmp(website,"ZZZZ") == 0){
				max = dumData-> avgUser;
				website = strdup(dumData->path+5);
			}
			else 
			if(dumData->avgUser == max){

				
				if(strcmp(dumData->path +5,website)<0){
					
					website = strdup(dumData->path+5);
				}
			}else{
				max = dumData->avgUser;
				website = strdup(dumData->path+5);
			}
			
		}
		dumData = dumData -> next;
	}
	printf("Result : %f, %s\n",max,website );
	return max;

}


void mostUsers2(){
	//places * dumPlaces = placesHead;
	dataCol2 *dumData = headCol2;
	
	//Make a char size 2 cause coutnry code only size 2
//	char * largestCount1;
	while(dumData!=NULL){
		char* country = strdup(dumData->maxCountry);
		int existsC = codeExist2(country);
        
        if(existsC == 1){
            places2 *dumPlaces = placesHead2;
            while(dumPlaces!= NULL){
                if((strcmp(country,dumPlaces->place)==0)){
                    dumPlaces ->counter = dumPlaces -> counter +dumData->countryCount ;   
                }
                dumPlaces = dumPlaces -> next;
                }
            }
        
        else{
           	//printf("%s\n", "we here?");
            places2 * dumPlaces2 = malloc(sizeof(places2));
            dumPlaces2->counter = dumData->countryCount;
            dumPlaces2 -> next = NULL;
            dumPlaces2->place = strdup(country);

            if(placesHead2==NULL){
                placesHead2 = dumPlaces2;
                curPlaces2 = placesHead2;
            }
            else{
                curPlaces2->next = dumPlaces2;
                curPlaces2 = dumPlaces2;
            }
        }
		dumData= dumData ->next;
	}


	int users = findLargestCountryInt2();
	char * place = strdup(findLargestCountry2());
	printf("Result %d, %s\n",users,place );	
}



