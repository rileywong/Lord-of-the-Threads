
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <semaphore.h>

//Synchronize this 
pthread_mutex_t headProtection;
    

struct dataCol4{
    double avgDur;
    //The year is useless take this out if you have time
    int year;
    char * path;
    double avgUser;
    char * maxCountry;
    int countryCount;
    struct dataCol4 * next;
};

struct years4{
	int year1;
	struct years4 * next;
};

struct places4{
	char * place;
	int counter;
	struct places4 * next;
};
struct placesRed4{
	char * place;
	int counter;
	struct placesRed4 * next;
};


typedef struct places4 places4;
typedef struct years4 years4;
typedef struct dataCol4 dataCol4;
typedef struct placesRed4 placesRed4;

struct dataCol4 * headCol4 = NULL;
struct dataCol4 * curCol4 = NULL;
struct dataCol4 * newCol;

struct places4 * placesHead4 = NULL;
struct places4 * curPlaces4 = NULL;

struct placesRed4 * placesHeadRed4 = NULL;
struct placesRed4 * curPlacesRed4 = NULL;

struct years4 * yearsHead4 = NULL;
struct years4 * curYears4 = NULL;

int whatYear4(char* timeStamp){
	time_t timeS = atoi(timeStamp);
        //printf("This is the timeS%lu\n", timeS);
       	struct tm *tmp;
        tmp = localtime(&timeS);
       // printf("This is the year : %d\n",tmp->tm_year +1900 );
        return tmp->tm_year +1900;
}



int nfiles4(char* dir){
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

void printList4(){
	dataCol4 * dum =headCol4;

	printf("Head address %s\n", headCol4->path);

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

int yearExist4(int year){
	years4 * dumYears = yearsHead4;
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

int codeExist4(char * code){
	places4 * dumPlaces = placesHead4;
	
	while(dumPlaces!=NULL){
		
		if((strcmp(code,dumPlaces->place)==0)){
			
			return 1;
		}
		dumPlaces = dumPlaces -> next;
	}
	
	return 0;
	
}
int codeExistRed4(char * code){
	placesRed4 * dumPlaces = placesHeadRed4;
	
	while(dumPlaces!=NULL){
		
		if((strcmp(code,dumPlaces->place)==0)){
			
			return 1;
		}
		dumPlaces = dumPlaces -> next;
	}
	
	return 0;
	
}

char * findLargestCountry4(){
	places4 * dumPlaces = placesHead4;
	int max = 0;
	//Make a char size 4 cause coutnry code only size 4
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

char * findLargestCountryRed4(){
	placesRed4 * dumPlaces = placesHeadRed4;
	int max = 0;
	//Make a char size 4 cause coutnry code only size 4
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


int findLargestCountryInt4(){
	places4 * dumPlaces = placesHead4;
	int max = 0;
	//Make a char size 4 cause coutnry code only size 4
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

int findLargestCountryIntRed4(){
	placesRed4 * dumPlaces = placesHeadRed4;
	int max = 0;
	//Make a char size 4 cause coutnry code only size 4
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


int maxAvgDur4(){
	dataCol4 * dumData = headCol4;
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

int minAvgDur4(){
	dataCol4 * dumData = headCol4;
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

int maxAvgUsers4(){
	dataCol4 * dumData = headCol4;
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

int minAvgUsers4(){
	dataCol4 * dumData = headCol4;
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


void mostUsers4(){
	//places * dumPlaces = placesHead;
	dataCol4 *dumData = headCol4;
	
	//Make a char size 4 cause coutnry code only size 4
//	char * largestCount1;
	while(dumData!=NULL){
		char* country = strdup(dumData->maxCountry);
		int existsC = codeExist4(country);
        
        if(existsC == 1){
            places4 *dumPlaces = placesHead4;
            while(dumPlaces!= NULL){
                if((strcmp(country,dumPlaces->place)==0)){
                    dumPlaces ->counter = dumPlaces -> counter +dumData->countryCount ;   
                }
                dumPlaces = dumPlaces -> next;
                }
            }
        
        else{
           	//printf("%s\n", "we here?");
            places4 * dumPlaces4 = malloc(sizeof(places4));
            dumPlaces4->counter = dumData->countryCount;
            dumPlaces4 -> next = NULL;
            dumPlaces4->place = strdup(country);

            if(placesHead4==NULL){
                placesHead4 = dumPlaces4;
                curPlaces4 = placesHead4;
            }
            else{
                curPlaces4->next = dumPlaces4;
                curPlaces4 = dumPlaces4;
            }
        }
		dumData= dumData ->next;
	}


	int users = findLargestCountryInt4();
	char * place = strdup(findLargestCountry4());
	printf("Result %d, %s\n",users,place );	
}

void mostUsersRed4(){

	//places4 * dumPlaces = placesHead4;

	int users = findLargestCountryInt4();
	char * place = strdup(findLargestCountry4());
	printf("Result %d, %s\n",users,place );	
}



