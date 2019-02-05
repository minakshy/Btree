#ifndef DBERROR_H
#define DBERROR_H

#include "stdio.h"

/* module wide constants */
#define PAGE_SIZE 4096

/* return code definitions */
typedef int RC;

#define RC_OK 0
#define RC_FILE_NOT_FOUND 1
#define RC_FILE_HANDLE_NOT_INIT 2
#define RC_WRITE_FAILED 3
#define RC_READ_NON_EXISTING_PAGE 4
#define RC_FILE_PRESENT 5
#define RC_FILE_CONTENTS_SAME 6
#define RC_FILE_NOT_DELETED 7
#define RC_FILE_POINTER_NOT_AT_FIRST 8
#define RC_FILE_NOT_OPEN 9
#define RC_READ_NOT_SUCCESSFULL 10
#define RC_WRITE_NOT_SUCCESSFULL 11
#define RC_INVALID_BUFFER 12
#define RC_NOT_FLUSH 13
#define RC_NO_SHUT_DOWN 15
#define RC_BUFFER_HAS_DIRTY_DATA 14
#define RC_BUFFER_NO_DIRTY 16
#define RC_NO_BUFFER_DATA 17
#define RC_DIRTY_BIT_NOT_CLEARED 18
#define RC_NOT_OK 19
#define RC_ERROR 20
#define RC_ALL_REMOVED 21
#define RC_ALGORITHM_NOT_IMPLEMENTED 21
#define RC_TABLE_NOT_DESTROYED 22 //hi 
#define RC_TABLE_NO_DATA 23
#define RC_SCHEMA_NULL 24
#define RC_RECORD_NULL 25
#define RC_INVALID_DATATYPE 26
#define RC_ID_INVALID 27
#define RC_TABLE_NOT_FOUND 28
#define RC_ALL_TUPLES_REMOVED 29
#define RC_FILE_FOUND 30
#define RC_NO_PIN 31
#define RC_UNPIN_ERROR 32
#define RC_PIN_ERROR 33
#define RC_NOATTR 34
#define RC_PINNED_PAGES_STILL_IN_BUFFER 35
#define RC_SIZE_NOT_OK 36
#define RC_TREE_EMPTY 37
#define RC_ID_NULL 38
#define RC_RM_COMPARE_VALUE_OF_DIFFERENT_DATATYPE 200
#define RC_RM_EXPR_RESULT_IS_NOT_BOOLEAN 201
#define RC_RM_BOOLEAN_EXPR_ARG_IS_NOT_BOOLEAN 202
#define RC_RM_NO_MORE_TUPLES 203
#define RC_RM_NO_PRINT_FOR_DATATYPE 204
#define RC_RM_UNKOWN_DATATYPE 205

#define RC_IM_KEY_NOT_FOUND 300
#define RC_IM_KEY_ALREADY_EXISTS 301
#define RC_IM_N_TO_LAGE 302
#define RC_IM_NO_MORE_ENTRIES 303

/* holder for error messages */
extern char *RC_message;

/* print a message to standard out describing the error */
extern void printError (RC error);
extern char *errorMessage (RC error);

#define THROW(rc,message) \
  do {			  \
    RC_message=message;	  \
    return rc;		  \
  } while (0)		  \

// check the return code and exit if it is an error
#define CHECK(code)							\
  do {									\
    int rc_internal = (code);						\
    if (rc_internal != RC_OK)						\
      {									\
	char *message = errorMessage(rc_internal);			\
	printf("[%s-L%i-%s] ERROR: Operation returned error: %s\n",__FILE__, __LINE__, __TIME__, message); \
	free(message);							\
	exit(1);							\
      }									\
  } while(0);


#endif
