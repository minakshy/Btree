#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "btree_mgr.h"
#include "tables.h"
#include "record_mgr.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"

// Structure for storing the B-tree attributes
typedef struct BTData                      
{  
    // no of nodes added
    int node_incr;	
    //nodes changed 								
	int node_change;
	RID *index;
    //value of node
	int node_value;
    int node_var;
	int expr;
	int *pointer;
    // size of the tree
	int node_capacity;
    // nodes that have been added 
	int nodes_inserted;
    // nodes in btree
	int nodes_enteries;
	int flag;
    int temp;
    int num_pages;
    int num_slots;
    int num_exchange;
	struct BTData **next_node;
}BTData;

//structure for storing the Buffer handle attributes
typedef struct BBuff
{
BM_BufferPool *bufferPool;
BM_PageHandle *handlePages;
}BBuff;

//initalising parent and bt object 
BTData *parent;
BTData bt;
SM_FileHandle fileHandle;
SM_PageHandle pageHandle;
BTData *duplicate;
BBuff *bbuff;

// we assign the respective sizes to a attribute
int size_bool = sizeof(bool);
int size_int = sizeof(int);
int size_char = sizeof(char);

// to check the status
int check_Open =0;

// we can try running to replace this #


/* Initialising the index manager */
RC initIndexManager (void *mgmtData)			   			    
{
	//return RC_NOT_OK;
	
    	// allocating memory for PageHandle and Parent
	pageHandle = malloc(4096 * size_int);	
	parent = (BTData*)malloc(sizeof(BTData));  
    	// reassuring if its initialised		
	if(parent)											
	{
		return RC_OK;								
	}
	return RC_FILE_NOT_FOUND;
	//return RC_OK;												
}
 

/*shutting down the index manager */
RC shutdownIndexManager ()			   				    		
{   
	//freeing the memory of pageHandle
	free(pageHandle);		
        //freeing the memory of parent									
	free(parent);
	//if(pageHandle==NULL && parent==NULL)
	return RC_OK;	
}											

/* Creating the btree with given key values */
RC createBtree (char *idxId, DataType keyType, int num)			
{   
	// the function takes nodes its datatype to create a btree
	if (parent!=NULL)										
	{
		// we create pointers and assign it with memory as well as for index and next_node		
		(*parent).pointer = malloc(size_int + size_char);					         
		(*parent).index = malloc(size_int + size_char);		          
		(*parent).next_node = malloc(size_int + size_int); 	
                // initializing it with zero			           
		bt.node_value=0;
	        //iterating from 0 to node_value + the size of bool
	        for(bt.node_value=0;bt.node_value<num+size_char;bt.node_value=bt.node_value+size_bool)
		{	
                     // here we initialize the btree handle to zero 									
		     (*parent).next_node[bt.node_value] = '\0';	
                     //setting the size to boolean 
		     bt.node_capacity = size_bool;									
													
                } 
                // we see if its datatype int                                               
		if(keyType== DT_INT)									
		{
			// creating a page file with name idxid							
			if(createPageFile (idxId) !=RC_OK)										
			{
				return RC_NOT_OK;		
	        	}
                	// opening the pageFile 
			if(openPageFile (idxId, &fileHandle)!=RC_OK)									
			{
				return RC_NOT_OK;		
			}
		}
        	// we increment the status after creating the pageful
		check_Open++;		
	 }
	return RC_OK;												
}

/* To Open a btree where the tree and name is passed as parameters */
RC openBtree (BTreeHandle **tree, char *idxId)			
{   
	// checks if the tree was created
	if (check_Open==1)	
	{
	   	//allocating memory for Bbuff 
		bbuff = (BBuff*)malloc(sizeof(BBuff));	
       		// returning If tree or name or pointer is NULL
		if(tree == NULL)
			return RC_TREE_EMPTY;
		if(idxId == NULL)
			return RC_ID_NULL;	
  
       		// if the memory is allocated		
		if(bbuff!=NULL)                          									
		{ 
			// allocating buffer pool and buffer pages memory 
		 	(*bbuff).bufferPool = ((BM_BufferPool *) malloc (sizeof(BM_BufferPool)));
		 	(*bbuff).handlePages = ((BM_PageHandle *) malloc (sizeof(BM_PageHandle)));	
        
       			//checking if bufferPool is initialized by calling initalBufferPool function
			if(initBufferPool((*bbuff).bufferPool, idxId, 3, RS_FIFO, NULL)!=RC_OK)
			return RC_NO_BUFFER_DATA;			
		}
       
		//calling the Pinpage function 
	    	pinPage((*bbuff).bufferPool, (*bbuff).handlePages,1);
			
		//return RC_NOT_OK;
		
       		//calling the UnpinPage function										
		unpinPage((*bbuff).bufferPool, (*bbuff).handlePages);
			
		//return RC_NOT_OK;
		
	}
	return RC_OK;
}

/* Closes the Btree that is opened */
RC closeBtree (BTreeHandle *tree)								
{      
	//checks if tree is valid 
	if(tree==NULL)											
	{
	     return RC_OK;
	}
        // reallocation of the parent node and the tree memory 
	free(parent);										                          
        free(tree);     

        //Calls the closePage file to close it                  			
        closePageFile(&fileHandle);	
				
	//return RC_BUFFER_HAS_DIRTY_DATA;
        //now we check if it got closed and the memory is freed 
        if(tree==NULL)
	   return RC_OK;
	//return RC_OK;												
}

/*	Deletes the given Btree */
RC deleteBtree (char *idxId)  
{   
    //removes the tree 
    remove(idxId);  
    return RC_OK;                                           
    
}

/* Function to calculate the number of nodes in the tree */
RC getNumNodes (BTreeHandle *tree, int *result)                
{   
	//initialising the value to zero 
        bt.nodes_enteries = 0;                                  
        //bt.node_value=0;
        //int no=1;
    
   	//iterating the value from zero to capacity + boolean size    
   	for(bt.node_value=0;bt.node_value < bt.node_capacity + size_bool;bt.node_value++)                      
   	{  
		//incrementing the inserted nodes by 1 
   	     bt.nodes_inserted = bt.nodes_inserted + 1;   
   	}
   	// assigning to the result pointer 
       *result = bt.nodes_inserted;                              
       //return *result;                                    
       return RC_OK;
}

/* Function to store the no of entries in a given tree */
RC getNumEntries (BTreeHandle *tree, int *result) 
{
    	bt.nodes_enteries = 0;                                 
    	bt.node_value=0;
   	//iterating the value from zero to capacity + boolean size 
   	for(bt.node_value=0;bt.node_value < bt.node_capacity + size_int;bt.node_value++)                      
        {   
	    //incrementing the inserted nodes by 1 
            bt.nodes_enteries = bt.nodes_enteries + 1;             
        }
        *result = bt.nodes_enteries;                              
        return RC_OK;
}

/* Function to return the keytype of a given tree */
RC getKeyType (BTreeHandle *tree, DataType *result)				
{
    	if(tree == NULL)
	return RC_TREE_EMPTY;
    	if(result == NULL)
	return RC_NOT_OK;
    	return RC_OK;
}
/* Function to searching the key in the btree*/
RC findKey (BTreeHandle *tree, Value *key, RID *result)			
{
	//RC rc;
	//Allocates the memory for the BTData as duplicate
	duplicate = (BTData*)malloc(sizeof(BTData));
	//intialises the flag to 0
	bt.flag = 0;												
	//rc =searchForKey(duplicate, key, result);
	//Start
	//Iterates to the next node by the size_bool till duplicate reaches null
	for (duplicate = parent; duplicate != NULL; duplicate = (*duplicate).next_node[size_bool])	
	{
		//bt.node_value=0;	
		//Initialises the node value to 0 and iterates till the size_int to check for all the enteries
		for(bt.node_value=0;bt.node_value<size_int;bt.node_value++)											
		{
			
			if ((*duplicate).pointer[bt.node_value] == (*key).v.intV) 		
			{
				//assigning the page value to the index
				(*result).page =(*duplicate).index[bt.node_value].page;		
				//assigning the slot value to the index
				(*result).slot = (*duplicate).index[bt.node_value].slot;		
				//Setting the flag & nodevalue to 1 to show success
				bt.flag = 1;										
				bt.node_value=1;											
				break;						
			}
			
		}
		//Checks if the flag is set or not
		if(bt.flag == 1)											
		break;
	}
	//END
	//After exiting if the flag is set returns all ok else invokes function to search through all the contents
	if(bt.flag == 1) 										
	return RC_OK;											
	else
	return RC_IM_KEY_NOT_FOUND;						
	
}

/* Function for inserting the key in the btree*/ 
RC insertKey (BTreeHandle *tree, Value *key, RID rid)				
{
	RC status;
	//allocating the size of BTData to a new variable 
	int treeSize =sizeof(BTData);	
	//initalising the counter as node_change to 0
	bt.node_change = 0;						
	//allocating the BTData memory
	duplicate = (BTData*)malloc(sizeof(BTData));
	//Iterates to the next node by the size_bool till duplicate reaches null		
	for (duplicate = parent; duplicate != NULL; duplicate = (*duplicate).next_node[size_bool])	
		{
	             //Makes a recursive call for insertion
		     //status=insertion(rid, key, tree);
		     BTData *ptr = (BTData*)malloc(sizeof(BTData));    \
	(*ptr).pointer = malloc(sizeof(int) + sizeof(char));             \
	(*ptr).index = malloc(sizeof(int) + sizeof(char));               \
	(*ptr).next_node = malloc(sizeof(int) + (sizeof(int))); 
	//initializing the node_value of the btree to 0
	bt.node_value=0;	
	//Checking each node value with the total size i.e bool
	while(bt.node_value<size_bool)													
	{
		
		if ((*duplicate).pointer[bt.node_value] == 0)								
		{
			//assigning the page value to the structure
			(*duplicate).index[bt.node_value].page = rid.page;	
			//assigning the slot value to the structure
			(*duplicate).index[bt.node_value].slot = rid.slot;
			//assigning the intV value to the structure
			(*duplicate).pointer[bt.node_value] = (*key).v.intV;
			//incrementing the node_change with the size of the bool
			bt.node_change = bt.node_change + size_bool;
			break;
		}
	    	bt.node_value++;														
	}
	if (bt.node_change == 0)												 
	{
		//checking for the adjacent node to be empty or not
		if((*duplicate).next_node[bt.node_capacity] == NULL)						
		{
			//assigning the pointer
			(*duplicate).next_node[bt.node_capacity] = ptr;						
		}
	}									

		}							
	
    	return RC_OK;														
}



/* Function for the removal of the key in the Btree*/
RC deleteKey (BTreeHandle *tree, Value *key)					
{
	//allocating the size of BTData to a new variable
	int handleSize = sizeof(BTData);				
	//checking for the size of BTData to be initialized or not
	if(handleSize>0)											
	{
		bt.flag = 0;											
		//recursivedelete(key,tree);
		//allocating the BTData memory
		duplicate = (BTData*)malloc(sizeof(BTData));
		//Iterates to the next node by the size_bool till duplicate reaches null
		for (duplicate = parent; duplicate != NULL; duplicate = (*duplicate).next_node[size_bool]) 	
		{
			//initialize node_value to 0
			bt.node_value = 0;	
			//Makes a recursive call to delete the key
			//keydeletion(key, tree);
			while(bt.node_value < size_int)												
	{
		if ((*duplicate).pointer[bt.node_value] == (*key).v.intV) 			
		{
			//assigning 0 to the structure including the page value & slot value
			(*duplicate).pointer[bt.node_value] = 0;							
			(*duplicate).index[bt.node_value].page = 0;					
			(*duplicate).index[bt.node_value].slot = 0;						
			bt.flag = 1;											
			break;
		}
		bt.node_value ++;													
	}											
			if (bt.flag == 1)												
        		{
				break;
        		}
		}								
	}
	return RC_OK;												
}


/* Function to scan the tree function*/
RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle)					
{
    	//allocating the BTData memory
	duplicate = (BTData*)malloc(sizeof(BTData));						
    	duplicate = parent;															
    	bt.temp = 0;
	//creating & allocating the memory for BTData var
    	BTData*node = (BTData*)malloc(sizeof(BTData));			
    	//initializing number of entries to 0
	bt.nodes_enteries = 0;
	//Iterates to the next node by the next max node value till node reaches null
    	for (node = parent; node != NULL; node = (*node).next_node[bt.node_capacity])	
      	{
        	for (bt.node_value = 0; bt.node_value < sizeof(int); bt.node_value ++)				
        	{
            		if ((*node).pointer[bt.node_value] != 0)								
             		{	
                		bt.nodes_enteries ++;			
             		}
         	}
      	}
	//invokes the scantree function for the values of the lookup
    	scanTree(tree, handle);													
    	return RC_OK;
}

/* Function to scan the tree value*/
RC scanTree(BTreeHandle *tree, BT_ScanHandle **handle)						
{	
	//creating & allocating the memory for BTData var
	BTData*node = (BTData*)malloc(sizeof(BTData));				
	int pointer[bt.nodes_enteries];											
    	int elements[bt.node_capacity][bt.nodes_enteries];	
    	bt.node_incr = 0;	
	//Iterates to the next node by the next max node value till node reaches null
    	for (node = parent; node != NULL; node = (*node).next_node[bt.node_capacity]) 
    	{
        	for (bt.node_value = 0; bt.node_value < sizeof(int); bt.node_value ++) 				
        	{
            	pointer[bt.node_incr] = (*node).pointer[bt.node_value];				
            	elements[0][bt.node_incr] = (*node).index[bt.node_value].page;		
            	elements[1][bt.node_incr] = (*node).index[bt.node_value].slot;		
		// increases the counter to scan the next value
            	bt.node_incr ++;												
        	}
    	}
}

/* Function for the next entry in the btree*/
RC nextEntry (BT_ScanHandle *handle, RID *result)							
{	 
	//BTreeHandle *b;
	//scanTree(b,handle);
    	//allocating the BTData memory
	duplicate = (BTData*)malloc(sizeof(BTData));		
	//checking if the entry is still available
    	if((bt.node_capacity == 0))													
    	{
		//assigning node capacity to the structure 
	  	duplicate = (*duplicate).next_node[bt.node_capacity];								 
      		//assigning page value to result page
	  	(*result).page = (*duplicate).index[bt.temp].page;
		(*result).page= (*result).page +6;			
      		//assigning slot value to the result page
	  	(*result).slot = (*duplicate).index[bt.temp].slot;
		(*result).slot = (*result).slot+6;						
      		bt.temp=bt.temp+sizeof(bool);	
		return RC_OK;											
    	}	
    	return RC_IM_NO_MORE_ENTRIES;															
}

/*Function for closing the tree scan*/
RC closeTreeScan (BT_ScanHandle *handle)									
{ 
   if(handle == NULL)
   return RC_NOT_OK;
    free(handle);
    return RC_OK;
}
/*Function for printing the tree acting as a test function*/
char *printTree (BTreeHandle* tree)											
{
    /*if(tree == NULL)
{
    printf("no such tree");
    return tree;
}*/
char *tree_str;
tree_str=printTree(tree);
printf("tree info:%s",tree_str);
return tree_str;/*
    return RC_TREE_EMPTY;
    printf("%c",&tree);
    return tree;*/
}         
