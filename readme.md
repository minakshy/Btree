.........................................................................................
ADO ASSIGNMENT 4 - BTREE MANAGER
..........................................................................................

-->Instruction to run
  Clear the screen and
  1. Navigate to the folder (assign4) where the assign4 files are present in the terminal 
  2. Run the make file with the command: make -f makefile.mk
  3. Then Execute : ./btree_mgr
  4. Run the make file with the command: make -f makef.mk
  5. Then Execute : ./test_expr
.........................................................................................
->Functions used :
/* We have added additional test cases as exceptions and we have also added additional error and return messages */
  Storage Manager:
  
  1.createPageFile :
    .open in read and write mode 
    .allocate memory 
    .check if file is Null ; if "File Present " else 
    .open file and write using seek
    .close and free the pointer and return "RC_OK"

  2.openPageFile :
    . open file in read mode
    . check if file is null ; if " file not found " else 
    . mgmtInfo gives the file pointer 
    . current position is zero and totalnumpages is 1 
    . closes and frees the pointer 

 3. closePageFile:
    .checks if file is null ; if file not found else 
    .opens in read mode 
    .closes the file using fclose
    .checks if file is closed and returns RC_OK
 
 4. destroyPageFile:
     .opens the file in read mode 
     .checks if null; if file not found else
     .remove the file 
     Check if file is removed if RC_OK else not deleted 
 
 5. readBlock:
     .open the file in read mode 
     .check if the mgmtInfo is null or not 
     . set the seeker to read the block 
     .checks if totalnumpages is less than pagenum 
     . if true then it reads the block with fread and the information from file pointer
     . set currpagepos to pagenum 
     .close the file 

 6. getBlockPos :
     .This function is to return the current position 

 7. readFirstBlock
     .open in read mode 
     .check if the file is present or not
     . use seek to point to the file and read the block 
     . same as read function 
     .but we set the currpagepos initially to zero and then point to the read_page 
 
 8. readPreviousBlock :
     .check if file present or not 
     .set the readpage to currpagepos-1
     .use seek and read the block 
     .Then same as read function 

 9. readCurrentBlock :
     .Same as read function except 
     .set read page using getblockpos function to retrieve the current page of the file 
 
 10. readNextBlock :
      . Same as read function except 
      . set the read page as curpagepos+1
 
 11. readLasttBlock :
      .Same as read function except 
      . read page is set to the last page (totalnumpages-1)

 12. WriteBlock :
     .open the file in write mode 
     . if file is null return file not found 
     . set write page using getblockpos function 
     . check if totalnumpages is less the pagenum
     . with seek write to file 
     .return ok and close the file 

 13. writeCurrentBlock :
      .open in write mode 
      . same as write block but here we will set write page to currentpagepos and also we check totalnumoages is less than write page
      .check if writing is successful else return write failed 

 14. appendEmptyBlock:
     .check if file is null or not 
     .open in append mode 
     . allocate the memory 
     .using seek and fwrite append a last block by incrementing totalnumpages by 1 
     .check if appended and the free the pointer 
 
 15. ensureCapacity :
      .check if file is present or not 
      . check if numofpages is more than totalnumpages 
      .increment the totalnumpages by numofpages 
      . check the insufficient size and append the pages using a while loop 

 Buffer Manager:

 1. initBufferPool:
	  . Define a struct Buffer_para and initialize values of pageNum, dirtypage, fixCount, lf (frequency of each page), lr (recently used variable for pages, data) to NULL.
	  . Initialize the buffer pool objects - pageFile, numPages, strategy with the parameter values called from the function. 
	  . Initialize write varibale which specifies whether a write operation has been done from the buffer pool to the storage manager to 0.
	  . Continue with this operation until the buffer has been filled until buffer_page_size variable.
	  
 2. forceFlushPool:
	  . Checks if the buffer is null, if so returns operation FAILED.
	  . Traverse through the buffer
	  . Check whether the particular buffer is both dirty and needs to be fixed.
	  . If so, then call openPageFile and write the contents of the buffer to the storage manager using writeBlock function
	  . Clear the dirtypages in the buffer by setting it to zero.
	  . Increment the write variable by one.
	  
 3. shutdownBufferPool:
	  . Checks if the buffer is null, if so returns operation FAILED.
	  . Call forceFlushPool function to write all the dirty pages in the buffer to the storage manager.
	  . Traverse through the buffer and check whether each buffer page has been fixed.
	  . If not, then return operation FAILED.
	  . If the above condition satisfies, then close the loop and return operation OK.
 
 4. markDirty:
      . Checks if the buffer is empty, if so does not return anything.
	  . Else, traverse through the buffer and check whether the current buffer's store page number and the storage's page number are the same, (i.e.) check whether 
	  . the buffer has been modified, if so set the dirtypage varible to 1 else to 0.

 5. forcePage:
	  . Checks if the buffer is empty, if so then return operation FAILED.
	  . Does the same operation as forceFlushPool, but forceFlushPool is only called only when the buffer is going to shut down.
	  . In forcePage, as soon as a dirty bit is found, the buffer contents are written onto the storage manager.
	  . Checks if the buffer page and storage manager page are same, and whether the buffer page has been modified.
	  . If so calls openPageFile and writes the contents of the buffer onto the page in the storage manager.
	  . Increments the write variable by one.
	
 6. getFrameContents:
	  . Checks whether the buffer is empty, if so returns the operation FAILED.
	  . Traverse through the buffer and get all the pagenumbers of the storage manager's block stored in the buffer.
	  . Return all the frames' contents stored in a seperate variable from the buffer.
	
 7. getDirtyFlags:
	  . Checks whether the buffer is empty, if so returns the operation FAILED.
	  . Traverse through the buffer and check whether the dirtypage variable of each buffer is set to 1.
	  . If so return true.
	  . Else return false.
	  
 8. getFixCounts:
	  . Checks if the buffer is empty, if so return  the operation FAILED.
	  . Traverse through the buffer and get all the fixCounts of each buffer.
	  . Store the values in an array.
	  . Return the array.

 9. getNumReadIO: 
	  . Returns the number of read operation performed on the disk.
	
 10. getNumWriteIO:
	  . Returns the number of write operation performed on the disk.
	  
 11. pinPage:
	  . This function is used to pin the page from the file to the buffer.
	  . Checks whether the buffer is empty.
	  . Checks the page replacement strategy written into the buffer struct and accordingly selects either FIFO, LFU or LRU.
 
 12. pinPageFIFO:
	  . Checks whether the buffer is empty and initializes pageNum, lf, lr, fixCount and data to the first buffer and sets everything to zero.
	  . For the next pages in the buffer, if the page has already been present fixCount, dirtypage and lr gets incremented and the page has been set to the first bit.
	  . else, appends a new page to the buffer and increments fixCount, lr by 1. 
	  . Then calls the FIFO method to implement the page replacement strategy.
	  
 13. FIFO: 
      . Implements the first in first out method with the help of a queue.
	  . Once a new buffer page arrives, and if the buffer is full, it deletes the page that was pushed to the buffer at first.
	  . Now the buffer that has pushed second will be updated to the first position.
	  . After another page has been added, the same method is implemented.
	  
 14. pinPageLRU
	  . Checks whether the buffer is empty and initializes pageNum, lf, lr, fixCount and data to the first buffer and sets everything to zero.
	  . For the next pages in the buffer, if the page has already been present fixCount, dirtypage and lf gets incremented and the page has been set to the first bit.
	  . else, appends a new page to the buffer and increments fixCount, lf by 1. 
	  . Then calls the LRU method to implement the page replacement strategy.
	  
 15. LRU:
	  . Implements the least recently used algorithm with the help of a stack.
	  . If the page that has been added to the buffer has not been repeated or used recently in the process the page will be deleted.
	  . The page that has been utilized most has been set to the top of the buffer and the page that has not been used recently resides at the bottom of the stack.
	  . Repeat the process until all pages has been added to the buffer.
	  
 16. pinPageLFU
	  . Checks whether the buffer is empty and initializes pageNum, lf, lr, fixCount and data to the first buffer and sets everything to zero.
	  . For the next pages in the buffer, if the page has already been present fixCount, dirtypage and lf, lr gets incremented and the page has been set to the first bit.
	  . else, appends a new page to the buffer and increments fixCount, lr and lr by 1. 
	  . Then calls the LRU method to implement the page replacement strategy.
	  
 17. LFU:
	  . Implements the least frequently used algorithm with the help of a stack.
	  . For the next set of pages, the program verifies whether each page in the buffer has been frequently used. update a pointer which maintains the frequency of each page in the buffer.
	  . If the page has been frequently used in the buffer, then keep it as it is when a new buffer has been added.
	  . If not, empty the page and append the new data in the buffer.
 
 18. unpinPage:
	  . Check whether the buffer is empty, if so return operation FAILED.
	  . Traverse through the buffer, find if the fixCount of each buffer is zero.
	  . If not, decrement the fixCount to zero.
	  . Delete the page in the buffer.
	  . Return true.
	  
 Record Manager:
 
 1.  initRecordManager:
      . Initializes a record manager instance for the program
	  . Return true
	  
 2.  shutdownRecordManager:
      . Shuts Down the record manager instance for the program
	  . Return true.
	  
 3.  createTable:
	  . Initialize a buffer pool instance with FIFO algorithm
	  . Set a page size to 4 bytes in page handle.
	  . Create and open a page file in the empty file "test_table_r"
	  . Write the first block of the file to '\0' bytes.
	  . Return true once this operation is done.

 4.  openTable:
	  . Use the table manager's buffer pool handle and pin the first page to the buffer pool using FIFO algorithm.
	  . Increment the number of pages handled by the buffer pool by 4 bytes.
	  . Initialize a new instance of schema memory and set its attribute names and datatypes to the number of pageshandled and the datatype specified.
	  . Unpin the page from the buffer pool
	  . Return true.
	  
 5.  closeTable:
      . Initialize the table manger to relational schema
	  . shutdown the buffer pool.
	  . Destroy the page 
	  . Return true.
	  
 6.  deleteTable:
	  . Destroy the buffer pool
	  . Destroy the page
	  . Destroy the file
	  . Check if everything mentioned above has been destroyed, return true
	  
 7.  insertRecord:
	  . Gets the record size we initialized at first.
	  . Gets the number of empty pages to which we can pin in the buffer.
	  . Pins the page into the buffer and the record manager.
	  . Get the number of records that are free in the record.
	  . Now pin the page to the empty space in the record using FIFO strategy.
	  . Check if there are any dirty pages in the buffer using markDirty function
	  . set a value to the particular key in the record.
	  . Unpin the page in the buffer.
	  . Pin the contents using FIFO strategy into the record manager.
	  . If all this is complete, return true.
	  
 8.  deleteRecord:
	  . Opens the page in the record.
	  . Unpins the page file from the buffer and record.
	  . Mark the page dirty if the page that has been changed in the buffer has not yet been updated in the record and the file.
	  . Unpin the page from the buffer.
	  . Destroy the page in the buffer.
	  . return true
	  
 9.  updateRecord:
	  . Get the particular record to which update has to be carried out.
      . Pin the page to the record and the buffer.
	  . Get the record size and update the value present in it. 
	  . Move the value to the record pointer.
	  . Mark the page dirty in the buffer and clear them using MarkDirty
	  . UnPin the page in the record.
	  . Return true.
	
 10. getRecord:
	  . Pin the page to the buffer and record from where it has to be fetched.
	  . Get the record pointer data of the page.
	  . Move the content from the record pointer to the memory.
	  . Unpin the page from the record.
	  . Return true.
	  
 11. startScan:
	  . Scan the particular record.
	  . Set the data to the scanner data.
	  . Set the key slot to 0.
	  . Set the key's page number to 1.
	  . Set the number of tuples in the record to 25.
	  . Set the relational schema
	  . End Scan
	  . Return true.
	  
 12. next:
	  . Get the record size of the records.
	  . Pin the pages in the record to the memory.
	  . Check if all the values in the record has been scanned.
	  . Check if the scanned values in the record are of either float or boolean type.
	  . If so, unpin the page from the record and return true.
	  . Else, unpin the page from the record and set the key's lot and page to 0 and 1 respectively.
	  . Return no more tuples scanned.
	  
 13. closeScan:
	  . Check whether the scanner has scanned all the records in the table.
	  . If so unpin the records and free the memory.
	  . Else set the data to '\0' bytes in the memory
	  . Free the data.
	  . Return true.
	  
 14. getRecordSize:
	  . Scan the records and check the datatype of schema present
	  . Increase the number of attributes in the record by the size of the datatype.
	  . Return the number pf attributes.
	  
 15. createSchema:
	  . Initialize a schema memory instance.
	  . Set the attributes of schema memory which are numAttr, attrNames, dataTypes, typeLength, keySize, keyAttrs to the parameters passed in the function.
	  . Return true if all value has been correctly set.
 
 16. freeSchema:
	  . Destroy the schema memory
	  . Update Record Size based on the datatype of the schema
	  . Return true.
	  
 17. createRecord:
	  . Set the record size initially to zero.
	  . Get the datatype of the schema
	  . Update the record size with respect to the size of the datatype of the relational schema.
	  . Create '\0' bytes in the record of 4 bytes.
	  . Return true after the record has been created.
	
 18. getAttr:
	  . Set a pointer data to the attribute number in the record.
	  . Based on the datatype of the schema, increase the record size accordingly.
	  . Set the value of the record based on the datatype of the schema.
	  . Return true.
	  
 19. setAttr:
	  . Get all the attr values from getAttr and sets them accordingly to temporary variables stored in the program.
	  . Sets al values accordingly 
	  . Return true.
	  
 20. getNumTuples:
	  . Get the number of tuples in the record in the table and return it accordingly.
	  
 BTree Manager:
 
 21. initIndexManager:
	  . Initialize the btree index manager.
	  . Return true.

 22. shutdownIndexManager:
	  . Shuts down the btree index manager.
	  
 23. createBtree:
	  . create a new node manager for btree index manager.
	  . if the manager is not empty, create a new page file using createpagefile with the btree index id.
	  . if not, then use openPagefile to open the btree index manager for the particular index id.
	  . Return true.

 24. openBtree:
	  . Initialize a buffer pool handle for the btree index.
	  . pinPage the buffer to the index using FIFO strategy.
	  . Return true.

 25. closeBtree:
	  . Free the memory space allocated for the btree index manager.
	  . Use closePageFile to close the buffer pool for the btree index.
	  . Return true.

 26. deleteBtree:
	  . remove the btree index
	  . Return true.

 27. getNumNodes:
	  . Traverse through the btree for all the search entries in the node manager.
	  . Increment the count by one.
	  . Store it in the *result parameter.
	  . Return true.
	  
 28. getNumEntries:
	  . Traverse through the btree and seach for all entries in the btree index manager.
	  . Increment the count by one.
	  . Store it in the *reuslt parameter.
	  . Return true.
	  
 29. getKeyType
	  . Check if the btree is empty.
	  . If not, then check the datatype of the keys in the btree whether they are DT_INT, DT_FLOAT, DT_BOOL or DT_STRING and store them in the datatype parameter.
	  . Return true.
 
 30. findKey:
	  . Traverse through the entire btree manager and search for the particular key to be search.
	  . Once the key has been found, note the index where the key has been found.
	  . Set the flag to 1. 
	  . Return true if flag is 1 else key not found error.

 31. insertKey:
      . The value, key and rid for the key to be inserted are given.
	  . Traverse through the btree to check where to insert the key.
	  . If the leaf node is full, split it and insert the key to the right nide.
	  . If not insert directly.
	  . Return true.

 32. deleteKey:
      . The value, key and rid for the key to be inserted are given.
	  . Traverse through the btree to check where to delete the key.
	  . If the leaf node after deletion is empty, free it.
	  . If the node contains only one entry, then combine it with other nodes using insertKey logic.
	  .Return true.

 33. openTreeScan:
      . Use the scan handle to scan the entire tree.
	  . Check for the node capcity and increment the pointer as you scan the entire tree.
	  . Return true.
	  
 34. nextEntry:
	  . Check if the node capcity is zero.
	  . If not, check for the next value in the tree from the index.
	  . Then store the value in result.
	  . UnPinPage from the btree buffer pool handle.
	  . Return true.

 35. closeTreeScan:
	  . free the buffer pool data.
	  . free the btree handle.
	  . Return true.
	  
 36. printTree:
	  . Prints the entire tree.
	  . Return true.

........................................................................................
->Team Members
   1)Minakshy Ramachandran A20396350 
   2)Nikita Jadhav A20401223
   3)Deepak Govind Mukundan A20396051
.........................................................................................






