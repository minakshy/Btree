#include<stdio.h>
#include<stdlib.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include<math.h>

// Create the structure for storing the buffer parameters
typedef struct Buffer_para {
	// Stores the information about the page & data field points to the content 
	SM_PageHandle data; 
	PageNumber pageNum;
	//frequently used
	int lf;
	// recently used  
	int lr;
	//indicates the page is modified
	int dirtypage;
	//indicates the num of users using the page
	int fixCount;
  
}Buffer_para;

Buffer_para *bpara;
Buffer_para *pin;

int buffer_page_size = 0;
SM_FileHandle fh;

int w = 0;  //for write I/O
int latter =0;
int hit = 0;

/* Initialise the buffer pool objects with parameter values */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,const int numPages, ReplacementStrategy strategy,void *stratData)
{
	//printf("Buffer Pool Initializing .... ");
	//Memory Allocation
	bpara = malloc(sizeof(Buffer_para)*numPages); 
	buffer_page_size =numPages;
	
	int i=0;
	//Initialising the structure Buffer_para parameters 
	while(i<buffer_page_size)
	{
		bpara[i].data = NULL;
		bpara[i].pageNum = -1;
		bpara[i].lf = 0;	
		bpara[i].lr = 0;
		bpara[i].dirtypage = 0;
		bpara[i].fixCount = 0;
		i++;
	}
	// passing the pageFileName parameters
	bm->pageFile = (char *)pageFileName;
	// passing the numPages parameters
	bm->numPages = numPages;
	// passing the page replacement strategy parameters
	bm->strategy = strategy; // passing parameters for the page replacement strateg
	// passing the buffer_para parameters
	bm->mgmtData = bpara;

	w = 0;

	return RC_OK;
}

/*Forcing values to shutdown the values in buffer pool*/
RC shutdownBufferPool(BM_BufferPool *const bm){
	//printf("Shut down...");
	if(bm->numPages<0)
	return RC_INVALID_BUFFER;

	bpara = (Buffer_para *)bm->mgmtData; 

	// if(bm->numPages<0)
	//return RC_INVALID_BUFFER;

	int flag = 0;
	//Invoking to force the values before shutting down
	if(forceFlushPool(bm)==RC_OK)
        flag=1; //sets the flag if successful

	int i=0;
	
	//Checks if the buffer has the dirty data or not
	while(i<buffer_page_size)
        {
		if(bpara[i].fixCount != 0)
		//flag = 1;
		//break;
		return RC_BUFFER_HAS_DIRTY_DATA;

		i++;
		//return RC_BUFFER_HAS_DIRTY_DATA;
	}

	free(bpara);
	bm->mgmtData = NULL;

	//if(flag == 1)
	//return RC_BUFFER_HAS_DIRTY_DATA;
	// else

        if(flag)
	return RC_OK;
}

/*Forcing the buffer pool contents*/
RC forceFlushPool(BM_BufferPool *const bm){
	//printf("Force flush...");
		
	if(bm->numPages<0)
	return RC_INVALID_BUFFER;
	//return RC_FILE_NOT_FOUND;

	int i=0;
	
	while(i<buffer_page_size)
	{
		//Checks for the dirty pages if the fixCount is 0
		if((bpara[i].dirtypage==1) && (bpara[i].fixCount ==0) )
		{
			openPageFile(bm->pageFile,&fh);
			//If fixCount is 0 write from bufferpool to disk			
			writeBlock(bpara[i].pageNum, &fh , bpara[i].data);
			//return RC_WRITE_NOT_SUCCESSFULL;

			//Sets the dirty bits to 0
			bpara[i].dirtypage = 0;
			w++;
		}
		i++;
	}

	//closePageFile(&fh);
	//return RC_FILE_PRESENT;

	return RC_OK;
}

/* Check if buffer manager & Page Handle's pagenum is the same */
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page){
	//printf("Mark Dirty...");
	if(bm->numPages<0)
	return RC_INVALID_BUFFER;
	
	bpara = (Buffer_para *)bm->mgmtData;
	int i;
	int flag=0;

	for(i=0;i<buffer_page_size;i++)
	{
		if(bpara[i].pageNum == page->pageNum)
		{
			//Sets the dirty page value		
			bpara[i].dirtypage = 1;
			flag=1;
			return RC_OK;  
		}     
	}   
	// return RC_ERROR;
	//if(flag == 1)
	//return RC_OK;
	//else
	if(!flag)
	return RC_BUFFER_NO_DIRTY;
}

/* Check if buffer manager & Page Handle's pagenum is the same & decrements the value of fixcount */
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page){
	//printf("unpin page...");
	if(bm->numPages<0)
	return RC_INVALID_BUFFER;

	if(bm->mgmtData == NULL)
	return RC_NO_BUFFER_DATA;

	bpara = (Buffer_para *)bm->mgmtData; 
	int i =0 ;
	int flag=0;

	while(i<buffer_page_size)
	{
		//decrements the fixcount for unpinning		
		if(bpara[i].pageNum == page->pageNum)
		{
		  bpara[i].fixCount--;
			flag=1;
		  break;
		}
		i++;
	}
	if(flag)
	return RC_OK;
}

/* writes the modified contents and sets the dirty page */
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page){
	//printf("force page...");	
	if(bm->mgmtData == NULL)
	return RC_NO_BUFFER_DATA;

	if(bm->numPages<=0)
	return RC_INVALID_BUFFER;

	if(openPageFile(bm->pageFile,&fh) != RC_OK)
	return RC_FILE_NOT_FOUND;

	// int flag =0;
	int i=0;

	while(i<buffer_page_size)
	{
		if(bpara[i].pageNum == page->pageNum )
		{
			//to write the current content			
			openPageFile(bm->pageFile,&fh);
			//return RC_NOT_OK if write not successful			
			if((writeBlock(bpara[i].pageNum, &fh , bpara[i].data)!=RC_OK))
			return RC_NOT_OK;
			bpara[i].dirtypage = 0;
			//flag = 1;
			w++; //page write results in write incrementation
		}
		i++;
	}

	return RC_OK;
	//else
	//  return RC_DIRTY_BIT_NOT_CLEARED;
}

/* Implements the First In First Out Replacement Strategy using Queue Data Structure */
void FIFO(BM_BufferPool *const bm, Buffer_para *node){
	//printf("FIFO...");
	int i=0;
	int j = latter%buffer_page_size;
	/*
	if(bm->mgmtData == NULL)
	return RC_NO_BUFFER_DATA;

	if(bm->numPages<=0)
	return RC_INVALID_BUFFER;*/

	bpara = (Buffer_para *) bm->mgmtData;

	while(i<buffer_page_size) 
	{
		if(bpara[j].fixCount == 0) 
	  	{
			if(bpara[j].dirtypage == 1)
			{
				openPageFile (bm->pageFile, &fh);
				//writes the contents to the disk				
				writeBlock (bpara[j].pageNum, &fh, bpara[j].data);
				//page write results in write incrementation
				w++;  
			}
		/* Sets the buffer_para contents to new contents*/ 
		bpara[j].data = node->data;
		bpara[j].fixCount = node->fixCount;
		bpara[j].dirtypage = node->dirtypage;
		bpara[j].pageNum = node->pageNum;
		break;
		}
		else
		{
			// previous pointer is incremented			
			j++;
			if(j%buffer_page_size == 0)
			j=0;
        	}
		i++;
	}
}

RC pinpageFIFO(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
	//printf("Pin page FIFO...");

	if(bm->mgmtData == NULL)
	return RC_NO_BUFFER_DATA;

	if(bm->numPages<=0)
	return RC_INVALID_BUFFER;

	bpara = (Buffer_para *)bm->mgmtData;
	if(bpara[0].pageNum == -1) 
	{
		if(openPageFile (bm->pageFile, &fh)!=RC_OK) return RC_NOT_OK;

		bpara[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
		if(ensureCapacity(pageNum,&fh)!=RC_OK) return RC_SIZE_NOT_OK;
		if(readBlock(pageNum, &fh, bpara[0].data)!=RC_OK) return RC_ERROR;
		//incrementing the fixcount in the buffer pool
		bpara[0].fixCount++;
		page->data = bpara[0].data;
		
		//Setting the latter and lr parameter of buffer pool to 0
		latter = 0;
		bpara[0].lr = 0;

		//Setting the hit and lf parameter of buffer pool to 0
		hit = 0;
		bpara[0].lf = hit;
	
		bpara[0].pageNum = pageNum;	
		page->pageNum = pageNum;

		return RC_OK;
	}		

	else
	{	
	int i=0;
	int checksum = 0;
	while(i<buffer_page_size)		
	{
	if(bpara[i].pageNum != -1)
	{	
		 if(bpara[i].pageNum == pageNum)  
		 { 
		 bpara[i].fixCount++;
		 page->data = bpara[i].data;

	    	 checksum = 1;
		 //incrementing the hit in the buffer pool
	    	 hit++;

		 page->pageNum = pageNum;

		 break;
		 }
	}
	else	
	{
	    if(openPageFile (bm->pageFile, &fh)!=RC_OK) return RC_NOT_OK;
	    bpara[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
	    if(readBlock(pageNum, &fh, bpara[i].data)!=RC_OK) return RC_ERROR;
	    
	    //Setting fixcount to 1
	    bpara[i].fixCount = 1;
	    page->data = bpara[i].data;

	    bpara[i].lr = 0;
	    latter++;
	
	    hit++;
	    checksum =1;
    	    	
	    bpara[i].pageNum = pageNum;
	    page->pageNum = pageNum;
	    
	    break;
	   
	}
	i++;
	}
	if(checksum == 0)
	{		
	    pin = (Buffer_para *)malloc(sizeof(Buffer_para));		
	    if(openPageFile (bm->pageFile, &fh)!=RC_OK) return RC_NOT_OK;
	    pin->data = (SM_PageHandle) malloc(PAGE_SIZE);
	    if(readBlock(pageNum, &fh, pin->data)!=RC_OK) return RC_ERROR;

	    pin->dirtypage = 0;		
	    pin->fixCount = 1;

	    pin->lr = 0;
	    latter++;
	    hit++;

	    pin->pageNum = pageNum;
	    page->pageNum = pageNum;
	    page->data = pin->data;			
	    //Redirects to the FIFO function
	       FIFO(bm,pin);
	} 
	    return RC_OK;     

	}	
		    
}

/* Implementation of Least Recently Used algorithm with the help of Stack Data Structure*/
void LRU(BM_BufferPool *const bm, Buffer_para *node)
{
	//printf("LRU...");
	/*
	if(bm->mgmtData == NULL)
	return RC_NO_BUFFER_DATA;

	if(bm->numPages<=0)
	return RC_INVALID_BUFFER;*/

	bpara=(Buffer_para *) bm->mgmtData;
	int i=0;
	int j; //points to the previous element
	int k; //points to the smallest element

	//computing and storing the least element
	while(i<buffer_page_size)
	{
		if(bpara[i].fixCount == 0)
		{
			j= i;
			k = bpara[i].lf;
			break;
		}
	i++;
	}
	i=j+1;
	//ensuring the element to be least or not
	while(i<buffer_page_size)	
	{
		if(bpara[i].lf < k)
		{
		j = i;
		k = bpara[i].lf;
		}
	i++;
	}

	if(bpara[j].dirtypage == 1) 
	{ 
		openPageFile (bm->pageFile, &fh);
		//writes the contents to the disk
		writeBlock (bpara[j].pageNum, &fh, bpara[j].data);
		//page write results in write incrementation
		w++;
	}
	/* Sets the buffer_para contents to new contents*/ 
	bpara[j].data = node->data;
	bpara[j].dirtypage = node->dirtypage;
	bpara[j].fixCount = node->fixCount;
	bpara[j].pageNum = node->pageNum;
	bpara[j].lf = node->lf;
}

RC pinpageLRU(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
   //printf("Pin page LRU....");
  if(bm->mgmtData == NULL)
	return RC_NO_BUFFER_DATA;

	if(bm->numPages<=0)
	return RC_INVALID_BUFFER;

	bpara = (Buffer_para *)bm->mgmtData;
	if(bpara[0].pageNum == -1) 
	{
		if(openPageFile (bm->pageFile, &fh)!=RC_OK) return RC_NOT_OK;

		bpara[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
		if(ensureCapacity(pageNum,&fh)!=RC_OK) return RC_SIZE_NOT_OK;
		if(readBlock(pageNum, &fh, bpara[0].data)!=RC_OK) return RC_ERROR;
		//incrementing the fixcount in the buffer pool
		bpara[0].fixCount++;
		page->data = bpara[0].data;
		
		//Setting the latter and lr parameter of buffer pool to 0
		latter = 0;
		bpara[0].lr = 0;

		//Setting the hit to 0 and lf parameter of buffer pool to hit
		hit = 0;
		bpara[0].lf = hit;
	
		bpara[0].pageNum = pageNum;	
		page->pageNum = pageNum;

		return RC_OK;
	}		
 else
	{	
	int i=0;
	int checksum = 0;
	while(i<buffer_page_size)		
	{
	if(bpara[i].pageNum != -1)
	{	
		 if(bpara[i].pageNum == pageNum)  
		 { 
		 bpara[i].fixCount++;
	    	 checksum = 1;
		 //incrementing the hit in the buffer pool
	    	 hit++;
                 bpara[i].lf=hit;
		 page->pageNum = pageNum;
		 page->data = bpara[i].data;
		 break;
		 }
	}
	else	
	{
	    if(openPageFile (bm->pageFile, &fh)!=RC_OK) return RC_NOT_OK;
	    bpara[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
	    if(readBlock(pageNum, &fh, bpara[i].data)!=RC_OK) return RC_ERROR;
	    page->pageNum = pageNum;
	    //Setting fixcount to 1
	    bpara[i].fixCount = 1;
            
	    bpara[i].lr = 0;
	    latter++;
	    checksum=1;
	    hit++;
 	    bpara[i].lf=hit;
            
	    bpara[i].pageNum = pageNum;
	    page->data = bpara[i].data;
	    break;
	    
	}
	i++;
	}
	if(checksum == 0)
	{		
	    pin = (Buffer_para *)malloc(sizeof(Buffer_para));		
	    if(openPageFile (bm->pageFile, &fh)!=RC_OK) return RC_NOT_OK;
	    pin->data = (SM_PageHandle) malloc(PAGE_SIZE);
	    if(readBlock(pageNum, &fh, pin->data)!=RC_OK) return RC_ERROR;

	    pin->dirtypage = 0;		
	    pin->fixCount = 1;

	    pin->lr = 0;
	    latter++;
	    hit++;

	    pin->lf=hit;
	    pin->pageNum = pageNum;
	    page->pageNum = pageNum;
	    page->data = pin->data;			
	    //Redirects to the LRU function
	       LRU(bm,pin);
	} 
	    return RC_OK;     

	}	
	  
}

/* Implements the Least Frequently Used with the help of Stack Structure*/
void LFU(BM_BufferPool *const bm, Buffer_para *node){
	//printf("LFU...");
	bpara=(Buffer_para *) bm->mgmtData;
	int i=0;
	int j;
	int k;
	
	/*
	if(bm->mgmtData == NULL)
	return RC_NO_BUFFER_DATA;

	if(bm->numPages<=0)
	return RC_INVALID_BUFFER;*/

	//computing and storing the least used element
	while(i<buffer_page_size)
	{
		if(bpara[i].fixCount == 0)
		{
			j= i;
			k = bpara[i].lr;
			break;
		}
		i++;
	}
	i=j+1;

	//ensuring the element to be least used or not
	while(i<buffer_page_size)	
	{
		if(bpara[i].lr < k)
		{
			j = i;
			k = bpara[i].lr;
		}
		i++;
	}
	if(bpara[j].dirtypage == 1) 
	{ 		
		openPageFile (bm->pageFile, &fh);
		//writes the contents to the disk	
		writeBlock (bpara[j].pageNum, &fh, bpara[j].data);
		//page write results in write incrementation		
		w++;
	}
	
	/* Sets the buffer_para contents to new contents*/ 
	bpara[j].data = node->data;
	bpara[j].pageNum = node->pageNum;
	bpara[j].dirtypage = node->dirtypage;
	bpara[j].fixCount = node->fixCount;
	bpara[j].lr = node->lr;
}

RC pinpageLFU(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
   //printf("Pin page LFU....");
  if(bm->mgmtData == NULL)
	return RC_NO_BUFFER_DATA;

	if(bm->numPages<=0)
	return RC_INVALID_BUFFER;

	bpara = (Buffer_para *)bm->mgmtData;
	if(bpara[0].pageNum == -1) 
	{
		if(openPageFile (bm->pageFile, &fh)!=RC_OK) return RC_NOT_OK;

		bpara[0].data = (SM_PageHandle) malloc(PAGE_SIZE);
		if(ensureCapacity(pageNum,&fh)!=RC_OK) return RC_SIZE_NOT_OK;
		if(readBlock(pageNum, &fh, bpara[0].data)!=RC_OK) return RC_ERROR;
		//incrementing the fixcount in the buffer pool
		bpara[0].fixCount++;
		page->data = bpara[0].data;
		
		//Setting the latter to 0 and lr parameter of buffer pool to hit
		latter = 0;
		bpara[0].lr = hit;

		//Setting the hit and lf parameter of buffer pool to 0
		hit = 0;
		bpara[0].lf = 0;
	
		bpara[0].pageNum = pageNum;	
		page->pageNum = pageNum;

		return RC_OK;
	}		
else
	{	
	int i=0;
	int checksum = 0;
	while(i<buffer_page_size)		
	{
	if(bpara[i].pageNum != -1)
	{	
		 if(bpara[i].pageNum == pageNum)  
		 { 
		 bpara[i].fixCount++;
		 page->data = bpara[i].data;

	    	 checksum = 1;
		 //incrementing the hit in the buffer pool
	    	 hit++;

		 page->pageNum = pageNum;

		 break;
		 }
	}
	else	
	{
	    if(openPageFile (bm->pageFile, &fh)!=RC_OK) return RC_NOT_OK;
	    bpara[i].data = (SM_PageHandle) malloc(PAGE_SIZE);
	    if(readBlock(pageNum, &fh, bpara[i].data)!=RC_OK) return RC_ERROR;
	    
	    //Setting fixcount to 1
	    bpara[i].fixCount = 1;
	    page->data = bpara[i].data;

	    bpara[i].lf = 0;
	    latter++;
	
	    hit++;
	    bpara[i].lr=hit;
	    checksum =1;
    	    	
	    bpara[i].pageNum = pageNum;
	    page->pageNum = pageNum;
	    
	    break;
	   
	}
	i++;
	}
	if(checksum == 0)
	{		
	    pin = (Buffer_para *)malloc(sizeof(Buffer_para));		
	    if(openPageFile (bm->pageFile, &fh)!=RC_OK) return RC_NOT_OK;
	    pin->data = (SM_PageHandle) malloc(PAGE_SIZE);
	    if(readBlock(pageNum, &fh, pin->data)!=RC_OK) return RC_ERROR;

	    pin->dirtypage = 0;		
	    pin->fixCount = 1;

	    pin->lr = 0;
	    latter++;
	    hit++;

	    pin->pageNum = pageNum;
	    page->pageNum = pageNum;
	    page->data = pin->data;			
	    //Redirects to the FIFO function
	       LFU(bm,pin);
	} 
	    return RC_OK;     

	}	
	}

/* Checks the page replacement stratgey and redirects to the respective strategy*/
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum){
	//printf("Pin page select...");
	if(bm == NULL)
	return RC_NO_BUFFER_DATA;
	
	//redirects to the pinpageFIFO	
	if(bm->strategy == RS_FIFO){
	return pinpageFIFO(bm, page,pageNum);
	printf("In Fifo");
	}

	//redirects to the pinpageLRU
	else if(bm->strategy == RS_LRU){
	return pinpageLRU(bm, page,pageNum);
	printf("In LRU");
	}

	//redirects to the pinpageLFU
	else if(bm->strategy == RS_LFU){
	return pinpageLFU(bm,page,pageNum);
	printf("In LFU");
	}
	//return RC_OK;

	//return RC_OK;
}

/* Gets the Frame Contents & return array*/
PageNumber *getFrameContents (BM_BufferPool *const bm){
	// printf("Frame contentns...");
	PageNumber *return_page_number = malloc(sizeof(PageNumber)*buffer_page_size);
	bpara= (Buffer_para *)bm->mgmtData;
	int i=0;

	while(i<buffer_page_size)
	{
	return_page_number[i] = bpara[i].pageNum;
	i++;
	}
	return return_page_number;
	//return RC_OK;
}

/* checks the dirty bit is one or not*/
bool *getDirtyFlags (BM_BufferPool *const bm){
	//printf("Get Dirty pages...");
	bool *dirtyflag = malloc(sizeof(bool)*buffer_page_size);

	bpara= (Buffer_para *)bm->mgmtData;

	int i;	
	for(i=0;i<buffer_page_size;i++) 
	{

	dirtyflag[i]=(bpara[i].dirtypage == 1) ? true:false;

	}	
	return dirtyflag;
  
}

/*Reads the pagenum into array and returns it*/
int *getFixCounts (BM_BufferPool *const bm){
	//printf("Get fix counts...");
	int *fixCount = malloc(sizeof(int)*buffer_page_size);

	bpara= (Buffer_para *)bm->mgmtData;

	int i;	
	for(i=0;i<buffer_page_size;i++) 
	{
	fixCount[i] = bpara[i].fixCount;
	}
  return fixCount;
}

/*returns the page read*/
int getNumReadIO (BM_BufferPool *const bm){
	//printf("Get READ IO...");
	if(bm == NULL)
	return 0;
	return latter+1;
}
	
/*returns the page written*/
int getNumWriteIO (BM_BufferPool *const bm){ 
	//printf("GETWRITE IO..."); 
	if(bm == NULL)
	return 0;
	return w;
}



