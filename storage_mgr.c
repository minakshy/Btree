#include<stdio.h>
#include "dberror.h"
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include<math.h>

#include "storage_mgr.h"
void initStorageManager(void)
{
	printf("Sets the storage manager");
}


// Function to create a page of size 1 and fills it with /0 
RC createPageFile(char *filename)
{
	//printf("Creating page file...");

	FILE *myfile = fopen(filename, "w+"); // opening file in read mode 
	
        char *text; 
        text =  (char *) calloc (PAGE_SIZE, sizeof(char)); //allocating multiple blocks of memory each of same size 
	 
	//int flag=0; 
	/*if(myfile == NULL){
		return RC_FILE_NOT_FOUND;
	}
	fwrite(text,sizeof(char),PAGE_SIZE,myfile);
	  
	  fclose(myfile);
	  free(text);
	  return RC_OK;*/
	if(myfile != NULL)
        {
			
		//RC_message = "FILE PRESENT";	
                 //myfile = fopen(filename,"w");
		
               fwrite(text,sizeof(char),PAGE_SIZE,myfile);
		fclose(myfile);
		free(text);
		return RC_OK; // returning file not present 
	}	
		
       /*else
        {
		//myfile = fopen(filename,"w"); // opening file in write mode 
		long int pos = ftell(myfile);
                // getting the position of the file
 
		 if(pos == SEEK_END)
                    {
			return RC_FILE_POINTER_NOT_AT_FIRST; // pointer not in the begining 
		    }

		else
                  {
		       fseek(myfile, 0, SEEK_SET); // set pointer to begining
		       fwrite(text,sizeof(char),PAGE_SIZE,myfile);// write /0
		       free(text);
		       fclose(myfile);
		       return RC_OK;
		  }
	}*/
     //  if(myfile == NULL){
		return RC_FILE_NOT_FOUND;
	//}
 	  //fseek(myfile, 0, SEEK_SET);
   	  //fwrite(text,sizeof(char),PAGE_SIZE,myfile);
	  
	  //fclose(myfile);
	  //free(text);
	  //return RC_OK;
	
	
}


// to open an existing file page , if successful the fields of the handle will be initialised with the information of opened file 
RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
	//printf("Opening page file...");
	
	char ch;
	int num_lines = 0;
        int page;

	FILE *myfile = fopen(fileName,"r+"); // opening the file in read mode 
	
	if(myfile != NULL)
               {
			//printf("Reading contetns.........");
		    	fHandle->mgmtInfo = myfile;
		    	fHandle->fileName = fileName;

			// setting pointer to begining  
                        fseek(fHandle->mgmtInfo,0,SEEK_SET);

			int start_pos = ftell(fHandle->mgmtInfo);
			fHandle->curPagePos = start_pos;
			//fseek(fHandle->mgmtInfo, fHandle->curPagePos , SEEK_END);
			//page = ftell(myfile);
			fwrite(fileName,PAGE_SIZE,0,myfile);
			//int num_pages = (page)/PAGE_SIZE; //calculates the total num of pages
			fHandle->totalNumPages = 1;
			//printf("%f",num_pages);

			fclose(myfile);
			//printf("Reading complete........");
			return RC_OK;
	
		}
	else
               {
		    return RC_FILE_NOT_FOUND;
	       }
	//return RC_OK;
	
}

//closes the open pge 
RC closePageFile (SM_FileHandle *fHandle)
{
	//printf("Closing page file...");
	if(fHandle != NULL)
            {

		
	      
		if(fopen(fHandle->fileName,"r")== NULL)
                   {
		      return RC_FILE_NOT_OPEN;
	             
	           }
                 //checks if the file is open or not 
		else
                    {
			 int c = fclose(fHandle->mgmtInfo);
	              if(!c)
	              return RC_OK;
	
	 
		    }
             }
	//fHandle->mgmtInfo = fopen(fHandle->fileName,"r");
	//fclose(fHandle->mgmtInfo);
	
	return RC_OK;
}

//destroys the page

 RC destroyPageFile (char *fileName)
{
	//printf("destroying...");
	//FILE *myfile = fopen(fileName , "r+");
        int rem;

	//if(myfile!= NULL)
          //   {
	          // fclose(myfile);
	         //  rem = remove(fileName);
		
	      
           //check if file destroyed 
	     if(remove(fileName) == -1)
	        return RC_FILE_NOT_DELETED;
	    else
	        return RC_OK;
	    // }
	//return RC_FILE_NOT_FOUND;
}


RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	FILE *myfile;
	myfile = fopen(fHandle->fileName, "r"); 
	if(myfile == NULL){
	return RC_FILE_NOT_FOUND;
	}
	fseek(myfile, (pageNum * PAGE_SIZE), SEEK_SET);
	if(fread(memPage, 1, PAGE_SIZE, myfile) > PAGE_SIZE){
	return RC_ERROR;
	}
	fHandle->curPagePos = ftell(myfile); 
	fclose(myfile); 
	return RC_OK;
}

//gives the current position of the block
int getBlockPos (SM_FileHandle *fHandle)
{
	if(fHandle!= NULL)
		return fHandle->curPagePos;
	else 
		return RC_FILE_NOT_FOUND;
}

// reads the first page of the block
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // printf("Read 1 block");
	FILE *myfile = fopen(fHandle->fileName,"r+");
	if(fHandle->mgmtInfo == NULL)
		return RC_FILE_NOT_FOUND;
	//fHandle->mgmtInfo = fopen(fHandle->fileName,"r+");
	int read_page =1;
	int seeker;
        fseek(myfile, (read_page+1)*PAGE_SIZE, SEEK_SET);
	int readBlock;
	//fHandle->curPagePos = 0;
	if( fHandle->totalNumPages < read_page)
		return RC_READ_NON_EXISTING_PAGE;

	//if(seeker == 0 && !(fHandle->totalNumPages < read_page ))
	//{
		readBlock = fread(memPage,sizeof(char),PAGE_SIZE,myfile);
		fHandle->curPagePos = ftell(myfile);
		fclose(myfile);
		// Unable to read the block
		if(!readBlock){
			//fclose(myfile);
			return RC_READ_NOT_SUCCESSFULL;
		}
			
		else{
			//fclose(myfile);
			return RC_OK;
		}
			
	//}
	// file containing less than pagenum pages
	//else 
	//	return RC_READ_NON_EXISTING_PAGE;
	
	/*return readBlock(0,fHandle,memPage);
	return RC_OK;*/
}

//
//reads previous block in relation to current position of the block
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
//printf("Read prev block");
	if(fHandle->mgmtInfo == NULL)
		return RC_FILE_NOT_FOUND;
	FILE *myfile = fopen(fHandle->fileName,"r");
	int read_page;
	fHandle->curPagePos = ftell(myfile);
        read_page=fHandle->curPagePos - 1;
	int seeker;
        fseek(myfile, (read_page-2)*PAGE_SIZE, SEEK_SET);
	int readBlock;
	fHandle->curPagePos = fHandle->curPagePos - 1;
	if( fHandle->totalNumPages < read_page)
		return RC_READ_NON_EXISTING_PAGE;
	//if(seeker == 0 && !(fHandle->totalNumPages < read_page ))
	//{
		readBlock = fread(memPage,sizeof(char),PAGE_SIZE,myfile);
		fHandle->curPagePos = ftell(myfile);
		fclose(myfile);
		// Unable to read the block
		if(!readBlock){
			
			return RC_READ_NOT_SUCCESSFULL;
		}
			
		else{
			//fclose(myfile);
			return RC_OK;
		}
				
	//}
	// file containing less than pagenum pages
	//else 
	//	return RC_READ_NON_EXISTING_PAGE;
	//return RC_OK;
}

//reads current block in relation to current position of the block
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
 //printf("Read c block");
	if(fHandle->mgmtInfo == NULL)
		return RC_FILE_NOT_FOUND;
FILE *myfile = fopen(fHandle->fileName,"r");
	int read_page ;
        read_page= getBlockPos(fHandle) ;
	int seeker;
	fHandle->curPagePos = ftell(myfile);
        fseek(myfile, (fHandle->curPagePos+1)*PAGE_SIZE, SEEK_SET);
	int readBlock;
	fHandle->curPagePos = ftell(myfile);
	if( fHandle->totalNumPages < read_page)
		return RC_READ_NON_EXISTING_PAGE;
	//if(seeker == 0 && !(fHandle->totalNumPages < read_page ))
	//{
		readBlock = fread(memPage,sizeof(char),PAGE_SIZE,myfile);
		fHandle->curPagePos = ftell(myfile);
		fclose(myfile);
		// Unable to read the block
		if(!readBlock)
			return RC_READ_NOT_SUCCESSFULL;
		else
			return RC_OK;
	//}
	// file containing less than pagenum pages
	//else 
	//	return RC_READ_NON_EXISTING_PAGE;
}

//reads the next block 
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
//printf("Read n block");
	if(fHandle->mgmtInfo == NULL)
		return RC_FILE_NOT_FOUND;
	int read_page;
	FILE *myfile = fopen(fHandle->fileName,"r");
        fHandle->curPagePos = ftell(myfile);
	fHandle->curPagePos = fHandle->curPagePos + 1;
	int seeker;
        fseek(myfile, PAGE_SIZE*(fHandle->curPagePos+1), read_page);
	int readBlock;
	
	if( fHandle->totalNumPages < read_page)
		return RC_READ_NON_EXISTING_PAGE;
	//if(seeker == 0 && !(fHandle->totalNumPages < read_page ))
             //   {
		readBlock = fread(memPage,sizeof(char),PAGE_SIZE,myfile);
		fHandle->curPagePos = ftell(myfile);
		fclose(myfile);
		// Unable to read the block
		if(!readBlock)
			return RC_READ_NOT_SUCCESSFULL;
		else
			return RC_OK;
	      //  }
	// file containing less than pagenum pages
	//else 
//return RC_READ_NON_EXISTING_PAGE;
}

//reads last block in relation to current position of the block
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
 //printf("Read  l block");
	if(fHandle->mgmtInfo == NULL)
		return RC_FILE_NOT_FOUND;
	int read_page;
	FILE *myfile = fopen(fHandle->fileName,"r");
        read_page =fHandle->totalNumPages - 1;
	int seeker;
        fseek(myfile, PAGE_SIZE*(read_page+1), SEEK_SET);
	int readBlock;
	//fHandle->curPagePos = fHandle->curPagePos - 1;
	if( fHandle->totalNumPages < read_page)
		return RC_READ_NON_EXISTING_PAGE;
	//if(seeker == 0 && !(fHandle->totalNumPages < read_page ))
	//{
		readBlock = fread(memPage,sizeof(char),PAGE_SIZE,myfile);
		fHandle->curPagePos = ftell(myfile);
		fclose(myfile);
		// Unable to read the block
		if(!readBlock)
			return RC_READ_NOT_SUCCESSFULL;
		else
			return RC_OK;

	//}
	// file containing less than pagenum pages
	//else 
	//	return RC_READ_NON_EXISTING_PAGE;
}







RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	FILE *myfile;
	if(fHandle->mgmtInfo == NULL)
	return RC_FILE_NOT_FOUND;
	int cur_pos = (pageNum)*PAGE_SIZE;
	myfile = fopen(fHandle->fileName, "r+"); 
	if(pageNum!=0)
	{ 
	fHandle->curPagePos = cur_pos;
	fclose(myfile);
	writeCurrentBlock(fHandle,memPage);   
	}
	else
	{ 
	fseek(myfile,cur_pos,SEEK_SET);  
	int i=0;
	while(i<PAGE_SIZE)
	{
	fputc(memPage[i],myfile);
	i++;
	}
	fHandle->curPagePos = ftell(myfile);
	fclose(myfile);
	} 
	return RC_OK;
}

//Write Current Block
RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	FILE *myfile;
	myfile = fopen(fHandle->fileName, "r+");
	if(fHandle->mgmtInfo == NULL)
	return RC_FILE_NOT_FOUND;
	int curPos;
	curPos = fHandle->curPagePos;
	fseek(myfile,curPos,SEEK_SET);
	fwrite(memPage,1,strlen(memPage),myfile);
	fHandle->curPagePos = ftell(myfile);
	fclose(myfile); 
	return RC_OK;
}

//Append Empty Block


// Incrementing the pages by one till last page filling with 0 bytes
RC appendEmptyBlock (SM_FileHandle *fHandle)
{
//printf("Rappend block");
	
	if(fHandle == NULL)
		return RC_FILE_NOT_FOUND;
	
	FILE *myfile = fopen(fHandle->fileName,"a+");
	fHandle->totalNumPages +=1;//increments totalnumpages by 1
	char *empty_block = (char *) calloc (PAGE_SIZE,sizeof(char));
	int seeker;
        fseek(myfile, (fHandle->totalNumPages+1)*PAGE_SIZE, SEEK_END); // goes to end of page to append 
	int append_block;
        append_block = fwrite(empty_block, PAGE_SIZE, sizeof(char), myfile);
	fclose(myfile);
        if(append_block == 1)
            {
		//fclose(myfile);
		return RC_OK;
	    }
	else 
		return RC_WRITE_FAILED;
	free(empty_block);
}
// Adjusting to the size of the number of pages
RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
//printf("ensuring...");
	

	if(fHandle->mgmtInfo == NULL)
		return RC_FILE_NOT_FOUND;
	
	if(numberOfPages > fHandle->totalNumPages)
             {
		int fill_pages ;
                fill_pages = numberOfPages - fHandle->totalNumPages; // calculates the remaining pages to be filled
		fHandle->totalNumPages += numberOfPages;
		while(fill_pages < fHandle->totalNumPages)
                     {
		       appendEmptyBlock(fHandle);
		       fill_pages++;
		      }
	     }
	
	return RC_OK;
}


