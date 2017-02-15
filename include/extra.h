#include <dirent.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <semaphore.h>


//Synchronize this 
pthread_mutex_t headProtection;
    

struct dataCol{
    double avgDur;
    //The year is useless take this out if you have time
    int year;
    char * path;
    double avgUser;
    char * maxCountry;
    int countryCount;
    struct dataCol * next;
};

struct years{
	int year1;
	struct years * next;
};

struct places{
	char * place;
	int counter;
	struct places * next;
};


typedef struct places places;
typedef struct years years;
typedef struct dataCol dataCol;

struct dataCol * headCol = NULL;
struct dataCol * curCol = NULL;
struct dataCol * newCol;

struct places * placesHead = NULL;
struct places * curPlaces = NULL;

struct years * yearsHead = NULL;
struct years * curYears = NULL;

int whatYear(char* timeStamp){
	time_t timeS = atoi(timeStamp);
        //printf("This is the timeS%lu\n", timeS);
       	struct tm *tmp;
        tmp = localtime(&timeS);
       // printf("This is the year : %d\n",tmp->tm_year +1900 );
        return tmp->tm_year +1900;
}



int nfiles(char* dir){
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
            totalFiles = totalFiles;
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

void printList(){
	dataCol * dum =headCol;

	printf("Head address %s\n", headCol->path);

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

int yearExist(int year){
	years * dumYears = yearsHead;
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

int codeExist(char * code){
	places * dumPlaces = placesHead;
	while(dumPlaces!=NULL){
		if((strcmp(code,dumPlaces->place)==0)){
			return 1;
		}
		dumPlaces = dumPlaces -> next;
	}
	return 0;
	
}

char * findLargestCountry(){
	places * dumPlaces = placesHead;
	int max = 0;
	//Make a char size 2 cause coutnry code only size 2
	char * largestCount1;
	while(dumPlaces!=NULL){
		if(dumPlaces->counter > max){
			max = dumPlaces->counter;
			largestCount1= strdup(dumPlaces->place);
		}
		dumPlaces = dumPlaces ->next;
	}
	//return null on failure, shouldnt really fail unless something above failed
	
	return largestCount1;
}

int findLargestCountryInt(){
	places * dumPlaces = placesHead;
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


int maxAvgDur(){
	dataCol * dumData = headCol;
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

int minAvgDur(){
	dataCol * dumData = headCol;
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

int maxAvgUsers(){
	dataCol * dumData = headCol;
	double max = 0;
	char * website;
	while(dumData!=NULL){
		if(dumData->avgUser>max){
			max = dumData->avgUser;
			website = strdup(dumData->path +5);
		}
		dumData = dumData -> next;
	}
	printf("Result : %f, %s\n",max,website );
	return max;

}

int minAvgUsers(){
	dataCol * dumData = headCol;
	double max = 10000;
	char * website;
	while(dumData!=NULL){
		if(dumData->avgUser<max){
			max = dumData->avgUser;
			website = strdup(dumData->path +5);
		}
		dumData = dumData -> next;
	}

	printf("Result : %f, %s\n",max,website );
	return max;

}


void mostUsers(){
	//places * dumPlaces = placesHead;
	dataCol *dumData = headCol;
	
	//Make a char size 2 cause coutnry code only size 2
//	char * largestCount1;
	while(dumData!=NULL){
		char* country = strdup(dumData->maxCountry);
		int existsC = codeExist(country);
        
        if(existsC == 1){
            places *dumPlaces = placesHead;
            while(headCol!= NULL){
                if((strcmp(country,dumPlaces->place)==0)){
                    dumPlaces ->counter = dumPlaces -> counter +dumData->countryCount ;   
                }
                headCol = headCol -> next;
                }
            }
        
        else{
           	printf("%s\n", "we here?");
            places * dumPlaces = malloc(sizeof(places));
            dumPlaces->counter = headCol->countryCount;
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
		dumData= dumData ->next;
	}


	//int users = findLargestCountryInt();
	//char * place = strdup(findLargestCountry());
	//printf("Result %d, %s\n",users,place );	
}



