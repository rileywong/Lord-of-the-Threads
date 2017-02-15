
#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <semaphore.h>

//Synchronize this 
pthread_mutex_t headProtection;
    

struct dataCol3{
    double avgDur;
    //The year is useless take this out if you have time
    int year;
    char * path;
    double avgUser;
    char * maxCountry;
    int countryCount;
    struct dataCol3 * next;
};

struct years3{
	int year1;
	struct years3 * next;
};

struct places3{
	char * place;
	int counter;
	struct places3 * next;
};
struct placesRed3{
	char * place;
	int counter;
	struct placesRed3 * next;
};


typedef struct places3 places3;
typedef struct years3 years3;
typedef struct dataCol3 dataCol3;
typedef struct placesRed3 placesRed3;

struct dataCol3 * headCol3 = NULL;
struct dataCol3 * curCol3 = NULL;
struct dataCol3 * newCol;

struct places3 * placesHead3 = NULL;
struct places3 * curPlaces3 = NULL;

struct placesRed3 * placesHeadRed3 = NULL;
struct placesRed3 * curPlacesRed3 = NULL;

struct years3 * yearsHead3 = NULL;
struct years3 * curYears3 = NULL;

int whatYear3(char* timeStamp){
	time_t timeS = atoi(timeStamp);
        //printf("This is the timeS%lu\n", timeS);
       	struct tm *tmp;
        tmp = localtime(&timeS);
       // printf("This is the year : %d\n",tmp->tm_year +1900 );
        return tmp->tm_year +1900;
}



int nfiles3(char* dir){
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

void printList3(){
	dataCol3 * dum =headCol3;

	printf("Head address %s\n", headCol3->path);

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

int yearExist3(int year){
	years3 * dumYears = yearsHead3;
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

int codeExist3(char * code){
	places3 * dumPlaces = placesHead3;
	
	while(dumPlaces!=NULL){
		
		if((strcmp(code,dumPlaces->place)==0)){
			
			return 1;
		}
		dumPlaces = dumPlaces -> next;
	}
	
	return 0;
	
}
int codeExistRed3(char * code){
	placesRed3 * dumPlaces = placesHeadRed3;
	
	while(dumPlaces!=NULL){
		
		if((strcmp(code,dumPlaces->place)==0)){
			
			return 1;
		}
		dumPlaces = dumPlaces -> next;
	}
	
	return 0;
	
}

char * findLargestCountry3(){
	places3 * dumPlaces = placesHead3;
	int max = 0;
	//Make a char size 3 cause coutnry code only size 3
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

char * findLargestCountryRed3(){
	placesRed3 * dumPlaces = placesHeadRed3;
	int max = 0;
	//Make a char size 3 cause coutnry code only size 3
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


int findLargestCountryInt3(){
	places3 * dumPlaces = placesHead3;
	int max = 0;
	//Make a char size 3 cause coutnry code only size 3
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

int findLargestCountryIntRed3(){
	placesRed3 * dumPlaces = placesHeadRed3;
	int max = 0;
	//Make a char size 3 cause coutnry code only size 3
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


int maxAvgDur3(){
	dataCol3 * dumData = headCol3;
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

int minAvgDur3(){
	dataCol3 * dumData = headCol3;
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

int maxAvgUsers3(){
	dataCol3 * dumData = headCol3;
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

int minAvgUsers3(){
	dataCol3 * dumData = headCol3;
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


void mostUsers3(){
	//places * dumPlaces = placesHead;
	dataCol3 *dumData = headCol3;
	
	//Make a char size 3 cause coutnry code only size 3
//	char * largestCount1;
	while(dumData!=NULL){
		char* country = strdup(dumData->maxCountry);
		int existsC = codeExist3(country);
        
        if(existsC == 1){
            places3 *dumPlaces = placesHead3;
            while(dumPlaces!= NULL){
                if((strcmp(country,dumPlaces->place)==0)){
                    dumPlaces ->counter = dumPlaces -> counter +dumData->countryCount ;   
                }
                dumPlaces = dumPlaces -> next;
                }
            }
        
        else{
           	//printf("%s\n", "we here?");
            places3 * dumPlaces3 = malloc(sizeof(places3));
            dumPlaces3->counter = dumData->countryCount;
            dumPlaces3 -> next = NULL;
            dumPlaces3->place = strdup(country);

            if(placesHead3==NULL){
                placesHead3 = dumPlaces3;
                curPlaces3 = placesHead3;
            }
            else{
                curPlaces3->next = dumPlaces3;
                curPlaces3 = dumPlaces3;
            }
        }
		dumData= dumData ->next;
	}


	int users = findLargestCountryInt3();
	char * place = strdup(findLargestCountry3());
	printf("Result %d, %s\n",users,place );	
}

void mostUsersRed3(){

	//places3 * dumPlaces = placesHead3;

	int users = findLargestCountryInt3();
	char * place = strdup(findLargestCountry3());
	printf("Result %d, %s\n",users,place );	
}



