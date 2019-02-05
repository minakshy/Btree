#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include<math.h>

#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "dt.h"

#include "buffer_mgr_stat.h"
#include "dberror.h"
#include "record_mgr.h"

/* Initialises the record manager*/
RC initRecordManager(void *mgmtData)
{
    return RC_OK;
}

/* Creating free space for the record manager*/
int freeSpace(char *val, int size)
{
	int value = 0;
	//Assigning the num_slots considering the ceil value
	int num_slots = ceil(4096/size);
	for(;value<num_slots;value++)
	{
		if(val[value * size] != '$')
		return value;
        }
 	return -1;
}

/* Structures defining the attributes used in the record manager*/
typedef struct temps
{
	int num_slots;
	int num_attr;
	int recordsize;
	int attrs;
	int pos;
	int values;
	int numPages;
	int strleng;
	int length_Page;
}temps;

temps temp;

SM_FileHandle handleFile;
SM_PageHandle pageshandled;

char *pointer;
//char *result;
char *val;
char *new_value;

// Setting attributes values for getting & setting the attributes
int int_val = DT_INT;
char string_val = DT_STRING;
float float_val = DT_FLOAT;
bool bool_val = DT_BOOL;

/* Structure defining the attributes for the Table information*/
typedef struct tableMgrInfo{
	
  int number_tuples;
  int table_id;
  int number_empty;
  char *tableName;
  BM_PageHandle handlePages;
  BM_BufferPool poolHandle;
}tableMgrInfo;

/* Structure defining the attributes required to scan the tuples*/
typedef struct Scanner{
  
  Expr *scannercond;
  int scanned_tuples;
  RID key;
  BM_PageHandle handlePages;
}Scanner;

RID *key;
tableMgrInfo *tminfo;
//Scanner *scanner;

/* Retrieve the number of rows from the record manager*/
int getNumTuples (RM_TableData *rel)
{
	if(rel == NULL)
	return 0;
	// returns the number of tuples
	return (((tableMgrInfo *)rel->mgmtData)->number_tuples);
}

/* Get the size of the record  */
int getRecordSize(Schema *schema)
{
	temp.attrs = 0;
	temp.values = 0;
	for(;temp.values<schema->numAttr;temp.values++)
	{
		// Incrementing the attribute offset by the type value(int/string)		
		if(schema->dataTypes[temp.values] == int_val)
		{
			temp.attrs = temp.attrs + 4;
		}
		else if(schema->dataTypes[temp.values] == string_val)
		{
			temp.attrs = temp.attrs + schema->typeLength[temp.values];
		}
	}
	temp.attrs = temp.attrs++;
	return temp.attrs;
}

/* Creates the table in the record manager*/
RC createTable(char *name, Schema *schema)
{

	if(name == NULL)
	return RC_FILE_NOT_FOUND;

	if(schema == NULL)
	return RC_SCHEMA_NULL;

	//Allocates the memory for the tableMgrInfo structure
	tminfo = ((tableMgrInfo *) malloc (sizeof(tableMgrInfo)));
	char size[4096];
	//intialises the PAGESIZE & setting it to BM_PageHandle
	char *pageshandle = size;
	memset(pageshandle,0,4096);

	//Initialises the buffer pool 
	initBufferPool(&tminfo->poolHandle, name, 50, RS_FIFO,NULL);
	temp.values = 0;
	for(temp.values=0; temp.values<=sizeof(char);temp.values++)
	{ 
		//Sets the number of tuples in BM_PageHandle
		*(int *) pageshandle = temp.values;
		pageshandle = pageshandle + 4;
	}

	*(int *) pageshandle = 3;
	pageshandle = pageshandle + 4;

	
	if(pageshandle > 0 )
	{

		for(;temp.values<=sizeof(char);temp.values++)
		{
			//Sets the datatypes			
			*(int *) pageshandle = (int) schema->dataTypes[temp.values];
			pageshandle = pageshandle + sizeof(char);
			//Sets the typelength
			*(int *) pageshandle = (int) schema->typeLength[temp.values];
			pageshandle = pageshandle + sizeof(char);
			temp.attrs = *(int *) pageshandle;
		}
		//Checks if the page file exists
		if(createPageFile(name)!=RC_OK)
		return RC_FILE_NOT_FOUND;
		//Checks if the page file is open or not
		if(openPageFile(name, &handleFile)!=RC_OK)
		return RC_FILE_NOT_FOUND;
		//Writes to the first block
		if(writeBlock(0, &handleFile, size)!=RC_OK)
		return RC_WRITE_NOT_SUCCESSFULL;
		return RC_OK;
	}

}

/* Opens the table*/
RC openTable (RM_TableData *rel, char *name)
{  
	if(name == NULL)
	return RC_FILE_NOT_FOUND;
	if(rel == NULL)
	return RC_SCHEMA_NULL;
	temp.values = 0;
	rel->mgmtData = tminfo;
	//Pins the page for reading in the buffer pool
	if((pinPage(&tminfo->poolHandle, &tminfo->handlePages, -1))!=RC_OK)
	return RC_NO_PIN;
	pageshandled = (char *) tminfo->handlePages.data;
	//Retrieves the number of rows
	tminfo->number_tuples = *(int *)pageshandled;
	pageshandled = pageshandled + 4;
	//Retrieves the number of empty slots
	tminfo->number_empty = *(int *)pageshandled;
	pageshandled = pageshandled + 4;
	//incrementing pageHandle by 4
	temp.attrs = *(int *) pageshandled;
	pageshandled = pageshandled + 4;

        //creating an object of schema 
	Schema *schemamem;
	schemamem = (Schema *) malloc(sizeof(Schema));
	schemamem->numAttr = temp.attrs;
        //allocating the size of chear by 3 times
	schemamem->attrNames = (char**) malloc (sizeof(char)*3);
	schemamem->dataTypes = (DataType*) malloc(sizeof(int)*3);
	schemamem->typeLength = (int *)malloc(sizeof(int)*3);

	for(temp.values=0;temp.values<=sizeof(char);temp.values++)
	{       //assigning the attrNames to char + int size
		schemamem->attrNames[temp.values] = (char *) malloc(sizeof(char)+sizeof(int));
	}

	for(;temp.values<=(sizeof(char)+sizeof(int));temp.values++)
	{       //assigning to the int size of pagesHandled 
		schemamem->dataTypes[temp.values]=*(int *)pageshandled;
		rel->schema = schemamem;
	}

	if(unpinPage(&tminfo->poolHandle, &tminfo->handlePages)!=RC_OK)
	return RC_NO_BUFFER_DATA;

	return RC_OK; 
}
/* CLoses the table in record manager*/
RC closeTable(RM_TableData *rel){ 
   if(rel==NULL)
 return RC_SCHEMA_NULL;
  //initialising buffer pool
  BM_BufferPool *poolHandle = (BM_BufferPool *)rel->mgmtData;
  //creating tableMgrInfo object
  tableMgrInfo *tminfo = rel->mgmtData;
  //closing it using shutdownBuffer Pool
  shutdownBufferPool(&tminfo->poolHandle);
   //return RC_DIRTY_BIT_NOT_CLEARED;
  return RC_OK;
}
/* Deleting table in Record Manager */
RC deleteTable(char *name){
  if(name == NULL)
   return RC_FILE_NOT_FOUND;
  //destroying the page file
  if(destroyPageFile(name)!=RC_OK)
   return RC_TABLE_NOT_DESTROYED;

  return RC_OK;
}

/*Inserting record in Record Manager */
RC insertRecord (RM_TableData *rel, Record *record){

  if(rel == NULL)
     return RC_TABLE_NO_DATA;
   if(record==NULL)
     return RC_RECORD_NULL;
  tminfo = rel->mgmtData;
  key = &record->id;
   //printf("Hi");
  //initialising the record size from schema
  temp.recordsize = getRecordSize(rel->schema);
  key->page = tminfo->number_empty;
  if(pinPage(&tminfo->poolHandle, &tminfo->handlePages, key->page)!=RC_OK)
  return RC_FILE_NOT_FOUND;
  //stores the data of handlePages 
  val = tminfo->handlePages.data;
  key->slot = freeSpace(val, temp.recordsize);
  while((key->slot)<0){
    key->page++;
    if(pinPage(&tminfo->poolHandle, &tminfo->handlePages, key->page)!=RC_OK)
     return RC_FILE_NOT_FOUND;
    val = tminfo->handlePages.data;
    //creating freespace
    key->slot = freeSpace(val, temp.recordsize);
  }
  if(markDirty(&tminfo->poolHandle, &tminfo->handlePages)!=RC_OK)
  return RC_BUFFER_NO_DIRTY;
  val = val + (key->slot * temp.recordsize);
  
  *val = '$';
  val++;
  //printf("HI1");
  //appending the memory value
  memmove(val,record->data+1, temp.recordsize);
  if(unpinPage(&tminfo->poolHandle,&tminfo->handlePages)!=RC_OK)
  return RC_UNPIN_ERROR;
  if(pinPage(&tminfo->poolHandle, &tminfo->handlePages,1)!=RC_OK)
  return RC_PIN_ERROR;
  return RC_OK;
}
/*Deletes a record */
RC deleteRecord(RM_TableData *rel, RID id){

if(rel == NULL)
  return RC_TABLE_NO_DATA;
/*if(id==NULL)
  return RC_ID_INVALID;*/
openPageFile(tminfo->tableName,&handleFile);
 // return RC_FILE_NOT_FOUND;
if(pinPage(&tminfo->poolHandle,&tminfo->handlePages,id.page)!=RC_OK)
  return RC_PIN_ERROR;
val=tminfo->handlePages.data;
//storing the object's tuples 
temp.values=tminfo->number_tuples;
temp.recordsize=getRecordSize(rel->schema);
temp.values=temp.values+(id.slot*temp.recordsize);
if(markDirty(&tminfo->poolHandle,&tminfo->handlePages)!=RC_OK)
return RC_BUFFER_NO_DIRTY;
if(unpinPage(&tminfo->poolHandle,&tminfo->handlePages)!=RC_OK)
return RC_UNPIN_ERROR;

return RC_OK;
}

/* Update record in record manager */
RC updateRecord(RM_TableData *rel, Record *record)
{
if(rel == NULL)
  return RC_TABLE_NO_DATA;
key=&record->id;
if(pinPage(&tminfo->poolHandle,&tminfo->handlePages,record->id.page)!=RC_OK)
return RC_PIN_ERROR;
val=tminfo->handlePages.data;
//calculating the size of the record
temp.recordsize=getRecordSize(rel->schema);
val=val + (key->slot*temp.recordsize);
val++;
memmove(val,record->data+1, temp.recordsize);
if(markDirty(&tminfo->poolHandle,&tminfo->handlePages)!=RC_OK)
return RC_BUFFER_NO_DIRTY;
if(unpinPage(&tminfo->poolHandle,&tminfo->handlePages)!=RC_OK)
return RC_UNPIN_ERROR;

return RC_OK;
}
 /* Retrieves the particular record*/
RC getRecord(RM_TableData *rel,RID id,Record *record)
{
if(rel == NULL)
  return RC_TABLE_NO_DATA;
//if(!id)
//return RC_ID_INVALID;
if(pinPage(&tminfo->poolHandle,&tminfo->handlePages,id.page)!=RC_OK)
return RC_FILE_NOT_FOUND;
temp.recordsize=getRecordSize(rel->schema);
pointer=tminfo->handlePages.data;
//increments pointer by slot*recordsize
pointer=pointer+((id.slot)*temp.recordsize);
val=record->data;
val++;
memmove(val,pointer+1, temp.recordsize-1);
record->id=id;
if(unpinPage(&tminfo->poolHandle,&tminfo->handlePages)!=RC_OK)
return RC_FILE_NOT_FOUND;

return RC_OK;
}
/* Starts scanning the record from table */
RC startScan(RM_TableData *rel,RM_ScanHandle *scan,Expr *cond)
{
if(rel == NULL)
  return RC_TABLE_NO_DATA;
char *name;
//opens the table to read the data
if(openTable(rel,name)!=RC_OK)
return RC_TABLE_NOT_FOUND;
//creating a scanner object sc 
Scanner *sc=(Scanner *) malloc(sizeof(Scanner));
scan->mgmtData=sc;
//assigning page
sc->key.page=1;
//assigning slot 
sc->key.slot=0;
sc->scannercond=cond;
tableMgrInfo *tminfo=rel->mgmtData;
//initialising the number_tuples
tminfo->number_tuples=25;
scan->rel=rel;
return RC_OK;
}
/*Scans the next record from the table */
RC next(RM_ScanHandle *scan,Record *record)
{
Scanner *sc=(Scanner*)scan->mgmtData;
tableMgrInfo *tm=(tableMgrInfo *)scan->rel->mgmtData;
//intialises the value memory 
Value *v = (Value *)malloc(sizeof(Value));
//assigning record size from schema relation
temp.recordsize=getRecordSize(scan->rel->schema);
//assigning slots by dividing 4096 by record size 
temp.num_slots=ceil(4096/temp.recordsize);
int i,j;
int flag =0;
sc->scanned_tuples = 0;
i=sc->scanned_tuples;
//i=0;
j=tm->number_tuples;
//iterating from scanned tuples to 25
while(sc->scanned_tuples<=tm->number_tuples)
{
sc->key.slot= sc->key.slot + 1;
if(pinPage(&tm->poolHandle,&sc->handlePages,sc->key.page)!=RC_OK)
return RC_PIN_ERROR;
//assigning new_val and icrementing to slot*recordsize
new_value=sc->handlePages.data;
new_value=new_value+(sc->key.slot*temp.recordsize);
record->id.page=sc->key.page;
record->id.slot=sc->key.slot;
val=record->data;
val = val +1;
memmove(val,new_value+1,temp.recordsize-1);
//evaluates the expression 
evalExpr(record,(scan->rel)->schema,sc->scannercond,&v);
sc->scanned_tuples++;
//if its a bool or float value
if((v->v.floatV==TRUE)||(v->v.boolV == TRUE)){
 flag = 1;
 break;

}

}

if(flag == 0){
unpinPage(&tm->poolHandle,&sc->handlePages);
  sc->key.page = 1;
  sc->key.slot = 0;
  sc->scanned_tuples = 0;
  // when no more tuples
  return RC_RM_NO_MORE_TUPLES;
}
else{
unpinPage(&tm->poolHandle,&sc->handlePages);
return RC_OK;
}

  
}

/*closing the scan function */
RC closeScan(RM_ScanHandle *scan){
//allocating memory
Scanner *sc=(Scanner*)scan->mgmtData;
tableMgrInfo *tm=(tableMgrInfo *)scan->rel->mgmtData;
//while scanned tuples is less than equal to 0 
if((sc->scanned_tuples <0) && (sc->scanned_tuples==0)){
 scan->mgmtData = '\0';
//freeing the memory
 free(scan->mgmtData);
 return RC_OK;
}
else{
 unpinPage(&tm->poolHandle,&sc->handlePages);
 sc->key.page = 1;
  sc->key.slot = 0;
  sc->scanned_tuples = 0;
return RC_OK;
} 
}

/* Creates the schema*/
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
 
 int numattrs = 0;
 Schema *schemaMemory; 
 // Allocates memory for the schema                                                        
 schemaMemory= (Schema*) malloc(sizeof(Schema)); 
 //Allocates memory & Sets the attributes of the Schema function                              
 schemaMemory->numAttr= numattrs;                                      
 schemaMemory->attrNames= (char**) malloc(sizeof(char*)*3);                    
 schemaMemory->dataTypes= (DataType*) malloc(sizeof(int)*3);                   
 schemaMemory->typeLength= (int*) malloc(sizeof(int)*3);                       
 
 schemaMemory->numAttr = numAttr;
 schemaMemory->attrNames = attrNames;
 schemaMemory->dataTypes = dataTypes;
 schemaMemory->typeLength = typeLength;
 schemaMemory->keySize = keySize;
 schemaMemory->keyAttrs = keys;
 return schemaMemory;
 
}

/* Frees the Schema from the record manager*/
RC freeSchema(Schema *schema)
{
	//Checks if the schema is empty or not	
	if(schema == NULL)
	return RC_SCHEMA_NULL;
	free(schema);
	return RC_OK;
}

/* Creates the Record in the record manager*/
RC createRecord(Record **record, Schema *schema)                       
{                
	if(schema==NULL)
	return RC_SCHEMA_NULL; 
	temp.num_attr;
	temp.num_attr = schema->numAttr;	                               
	int *size = schema->typeLength;	
	//int values;				       
	temp.values=0;	
	temp.recordsize = 0;  //temp.recordsize
	//Initialises the Schema's DataTypes						       
	DataType *dataType = schema->dataTypes;
					       
	while(temp.values < sizeof(int)) 					       
	{

		temp.pos= *(dataType + temp.values);
 		//Checks if the pos consists of int/float/bool/string val type & sets the record size		                       
		if (temp.pos == int_val) 				       
		{
		temp.recordsize = temp.recordsize + sizeof(int);		       
		} 
		else if (temp.pos == float_val)			       
		{
		temp.recordsize = temp.recordsize + sizeof(int);		        
		}
		else if (temp.pos == bool_val) 			       
		{
		temp.recordsize = temp.recordsize + 1;			       
		} 
		else if (temp.pos == string_val)
		{ 
		temp.recordsize = temp.recordsize + (*(size + temp.values));	       
		}
		temp.values++;						               
	}
	temp.values=0;							       
	while(temp.values < sizeof(int) + sizeof(char))			       
	{
		//allocates memory to the pointer variable 		
		pointer = (char *)malloc(sizeof(int) + sizeof(char));		       
		pointer[temp.values]='\0';
		//allocates memory to the record variable						       
		*record = (Record *)malloc(sizeof(int));			       
		record[0]->data=pointer;					       
		temp.values++;							       
	}
	return RC_OK;								
}

/* Frees the record from record manager*/
RC freeRecord (Record *record)
{
	if(record==NULL)
	return RC_RECORD_NULL;
	free(record);                                      			                      
	return RC_OK;								
}

/*Performs getters & setters operation on the schema attributes*/
int get_set_attributes(Schema *schema, int attrNum, int *final)
{
	if(schema==NULL)
	return RC_SCHEMA_NULL; 

	temp.attrs = 1;
	temp.pos =0;
	//Iterating from 0 to attrNum
	for(temp.pos = 0;temp.pos<attrNum;temp.pos++)
	{
		//Selects the datatypes		
		switch(schema->dataTypes[temp.pos])
		{
		case DT_STRING: temp.attrs = temp.attrs + schema->typeLength[temp.pos];
		break;
		case DT_INT: temp.attrs = temp.attrs + sizeof(int); break;
		case DT_FLOAT: temp.attrs = temp.attrs + sizeof(float); break;
		case DT_BOOL:
		temp.attrs = temp.attrs + sizeof(bool);
		break;
		}
	}
	*final = temp.attrs;
	return RC_OK;
}

/* Retrieves the attributes from the record manager*/
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) 
{
	if(schema==NULL)
	return RC_SCHEMA_NULL;

	temp.values = 0;
	temp.attrs = 0; 	
	temp.strleng =0;
			    
	if(get_set_attributes(schema, attrNum, &temp.attrs)!=RC_OK)
	return RC_NOATTR; 
 	//Allocates memory to the value variable
	Value *v;                                                                     
	v = (Value *) malloc(sizeof(Value));					     
	pointer = record->data;					     
	pointer = pointer + temp.attrs;	
	
	//	     
	if(attrNum == 1)					     
	{
	schema->dataTypes[attrNum] = 1;				     
	}
        //if the datatype val is INT
	if (schema->dataTypes[attrNum] == int_val)		     
	{
	memmove(&temp.values,pointer, 4);				    
	v->v.intV = temp.values;					     
	v->dt = DT_INT;
	}
        //if the datatype val is string
	else if (schema->dataTypes[attrNum] == string_val)	     
	{
	v->dt = DT_STRING;					     
	temp.strleng = schema->typeLength[attrNum];	     
	v->v.stringV = (char *) malloc(sizeof(char));		     
	//int strleng;
	strncpy(v->v.stringV, pointer, temp.strleng);	    
	v->v.stringV[temp.strleng] = '\0';			     
	}
        //if the datatype value is Float
	else if (schema->dataTypes[attrNum] == float_val)	     
	{
	v->dt = DT_FLOAT;					     
	float floatingVal;					     
	memmove(&floatingVal,pointer,4);			     
	v->v.floatV = floatingVal;				     
	}
        //if it is Bool value
	else if (schema->dataTypes[attrNum] == bool_val)	     
	{
	v->dt = DT_BOOL;					     
	bool booleanVal;					    
	memmove(&booleanVal,pointer,1);			     
	v->v.boolV = booleanVal;				      
	}
	else
	{
	return RC_INVALID_DATATYPE;				     
	}
	*value = v;						     
	return RC_OK;						     
}
/*Setting values to the record and schema*/
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
if(schema==NULL)
return RC_SCHEMA_NULL;
temp.attrs = 0;			              
get_set_attributes(schema, attrNum, &temp.attrs);
//int recordsize;  
// calls function to assign value
pointer = record->data;
//increment the pointer by attrs					      
pointer = pointer + temp.attrs;	
//if the datatye is int Val	      
if (schema->dataTypes[attrNum] == int_val)		      
{
*(int *)pointer = value->v.intV;	  		      
}
//if the datatype is string val
else if(schema->dataTypes[attrNum] == string_val)	      
{
temp.recordsize = schema->typeLength[attrNum];
//alloacating the new value to size of char and int  	      
new_value = (char *) malloc(sizeof(char)+sizeof(int));
//copying the value into new_val variable	      
strncpy(new_value, value->v.stringV, sizeof(char)+sizeof(int));
new_value[temp.recordsize] = '\0';	
//copying new_value to the pointer		      
strncpy(pointer, new_value, sizeof(char)+sizeof(int));	      
}
//if datatype is float
else if(schema->dataTypes[attrNum] == float_val)	      
{
float floatingVal;					      
floatingVal = value->v.floatV;	
//incrementing by 4 		      
floatingVal = floatingVal + 4;			     
}
//if its a bool variable
else if(schema->dataTypes[attrNum] == bool_val)	      
{
bool booleanVal;					      
booleanVal = value->v.boolV;				      
booleanVal = booleanVal++;				     
}
else
{
return RC_INVALID_DATATYPE;				      
}
return RC_OK;						      
}

// Shutdows the Record manager
RC shutdownRecordManager ()
{
    return RC_OK;
}


