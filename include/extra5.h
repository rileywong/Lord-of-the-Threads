
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <semaphore.h>

//Synchronize this 
pthread_mutex_t headProtection;
    

struct dataCol5{
    double avgDur;
    //The year is useless take this out if you have time
    int year;
    char * path;
    double avgUser;
    char * maxCountry;
    int countryCount;
    struct dataCol5 * next;
};

struct years5{
	int year1;
	struct years5 * next;
};

struct places5{
	char * place;
	int counter;
	struct places5 * next;
};
struct placesRed5{
	char * place;
	int counter;
	struct placesRed5 * next;
};


typedef struct places5 places5;
typedef struct years5 years5;
typedef struct dataCol5 dataCol5;
typedef struct placesRed5 placesRed5;

struct dataCol5 * headCol5 = NULL;
struct dataCol5 * curCol5 = NULL;
struct dataCol5 * newCol;

struct places5 * placesHead5 = NULL;
struct places5 * curPlaces5 = NULL;

struct placesRed5 * placesHeadRed5 = NULL;
struct placesRed5 * curPlacesRed5 = NULL;

struct years5 * yearsHead5 = NULL;
struct years5 * curYears5 = NULL;

int whatYear5(char* timeStamp){
	time_t timeS = atoi(timeStamp);
        //printf("This is the timeS%lu\n", timeS);
       	struct tm *tmp;
        tmp = localtime(&timeS);
       // printf("This is the year : %d\n",tmp->tm_year +1900 );
        return tmp->tm_year +1900;
}



int nfiles5(char* dir){
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

void printList5(){
	dataCol5 * dum =headCol5;

	printf("Head address %s\n", headCol5->path);

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

int yearExist5(int year){
	years5 * dumYears = yearsHead5;
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

int codeExist5(char * code){
	places5 * dumPlaces = placesHead5;
	
	while(dumPlaces!=NULL){
		
		if((strcmp(code,dumPlaces->place)==0)){
			
			return 1;
		}
		dumPlaces = dumPlaces -> next;
	}
	
	return 0;
	
}
int codeExistRed5(char * code){
	placesRed5 * dumPlaces = placesHeadRed5;
	
	while(dumPlaces!=NULL){
		
		if((strcmp(code,dumPlaces->place)==0)){
			
			return 1;
		}
		dumPlaces = dumPlaces -> next;
	}
	
	return 0;
	
}

char * findLargestCountry5(){
	places5 * dumPlaces = placesHead5;
	int max = 0;
	//Make a char size 5 cause coutnry code only size 5
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

char * findLargestCountryRed5(){
	placesRed5 * dumPlaces = placesHeadRed5;
	int max = 0;
	//Make a char size 5 cause coutnry code only size 5
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


int findLargestCountryInt5(){
	places5 * dumPlaces = placesHead5;
	int max = 0;
	//Make a char size 5 cause coutnry code only size 5
	//char * largestCount1;
	while(dumPlaces!=NULL){
		//printf("%s\n", dumPlaces->place);
		if(dumPlaces->counter > max){
			max = dumPlaces->counter;
			//largestCount1= strdup(dumPlaces->place);
		}
		dumPlaces = dumPlaces ->next;
	}
	//return null on failure, shouldnt really fail unless something above failed
	
	return max;
}

int findLargestCountryIntRed5(){
	placesRed5 * dumPlaces = placesHeadRed5;
	int max = 0;
	//Make a char size 5 cause coutnry code only size 5
	//char * largestCount1;
	while(dumPlaces!=NULL){
		//printf("%s\n", dumPlaces->place);
		if(dumPlaces->counter > max){
			max = dumPlaces->counter;
			//largestCount1= strdup(dumPlaces->place);
		}
		dumPlaces = dumPlaces ->next;
	}
	//return null on failure, shouldnt really fail unless something above failed
	
	return max;
}


int maxAvgDur5(){
	dataCol5 * dumData = headCol5;
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

int minAvgDur5(){
	dataCol5 * dumData = headCol5;
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

int maxAvgUsers5(){
	dataCol5 * dumData = headCol5;
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

int minAvgUsers5(){
	dataCol5 * dumData = headCol5;
	double max = 10000;
	char * website= strdup("ZZZZ");
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


void mostUsers5(){
	//places * dumPlaces = placesHead;
	dataCol5 *dumData = headCol5;
	
	//Make a char size 5 cause coutnry code only size 5
//	char * largestCount1;
	while(dumData!=NULL){
		char* country = strdup(dumData->maxCountry);
		int existsC = codeExist5(country);
        
        if(existsC == 1){
            places5 *dumPlaces = placesHead5;
            while(dumPlaces!= NULL){
                if((strcmp(country,dumPlaces->place)==0)){
                    dumPlaces ->counter = dumPlaces -> counter +dumData->countryCount ;   
                }
                dumPlaces = dumPlaces -> next;
                }
            }
        
        else{
           	//printf("%s\n", "we here?");
            places5 * dumPlaces5 = malloc(sizeof(places5));
            dumPlaces5->counter = dumData->countryCount;
            dumPlaces5 -> next = NULL;
            dumPlaces5->place = strdup(country);

            if(placesHead5==NULL){
                placesHead5 = dumPlaces5;
                curPlaces5 = placesHead5;
            }
            else{
                curPlaces5->next = dumPlaces5;
                curPlaces5 = dumPlaces5;
            }
        }
		dumData= dumData ->next;
	}


	int users = findLargestCountryInt5();
	char * place = strdup(findLargestCountry5());
	printf("Result %d, %s\n",users,place );	
}

void mostUsersRed5(){

	//places5 * dumPlaces = placesHead5;

	int users = findLargestCountryInt5();
	char * place = strdup(findLargestCountry5());
	printf("Result %d, %s\n",users,place );	
}



