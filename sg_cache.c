////////////////////////////////////////////////////////////////////////////////
//
//  File           : sg_driver.c
//  Description    : This file contains the driver code to be developed by
//                   the students of the 311 class.  See assignment details
//                   for additional information.
//
//   Author        : Yao Xu
//   Last Modified : 
//

// Include Files
#include <stdlib.h>
#include <cmpsc311_log.h>
#include <stdbool.h>

// Project Includes
#include <sg_cache.h>
#include <string.h>

// Defines
int latest_time = 1;
uint16_t maxElementsRecord;
uint16_t cacheElementsCount = 0;
int total = 0;
int hit = 0;

typedef struct {    //cache line strcture
    int time;
    SG_Node_ID node_ID;
    SG_Block_ID blk_ID;
    char data [SG_BLOCK_SIZE];
} SG_cache_line;

SG_cache_line * cache;
// Functional Prototypes

//
// Functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : initSGCache
// Description  : Initialize the cache of block elements
//
// Inputs       : maxElements - maximum number of elements allowed
// Outputs      : 0 if successful, -1 if failure

int initSGCache( uint16_t maxElements ) {
    maxElementsRecord = maxElements;
    if ( maxElements <= 0 ){
        logMessage( LOG_ERROR_LEVEL, "initSGCache: invalid cache size: [%d].", maxElements );
        return (-1);
    } else {
        cache = (SG_cache_line *) calloc(maxElements, sizeof(SG_cache_line));   //allocating cache
        if ( cache == NULL ){
            logMessage( LOG_ERROR_LEVEL, "initSGCache: memory allocation failed. " );
            return (-1);
        }
        // Return successfully
        return( 0 );
    }
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : closeSGCache
// Description  : Close the cache of block elements, clean up remaining data
//
// Inputs       : none
// Outputs      : 0 if successful, -1 if failure

int closeSGCache( void ) {
    free(cache);        // free allocated memory
    cache = NULL;
    float rate = ((float)hit/(float)total)*100;
    logMessage( LOG_INFO_LEVEL, "[Cache] Total queries: %d, hit count: %d, hit rate: %f%%", total, hit, rate );
    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : getSGDataBlock
// Description  : Get the data block from the block cache
//
// Inputs       : nde - node ID to find
//                blk - block ID to find
// Outputs      : pointer to block or NULL if not found

char * getSGDataBlock( SG_Node_ID nde, SG_Block_ID blk ) {
    total += 1;             // count total queries
    
    if ( nde==0 && blk==0 ){
        logMessage( LOG_ERROR_LEVEL, "[cache] getSGDataBlock: invalid node or blk ID. " );
        return (NULL);
    }
    for ( int i=0; i<maxElementsRecord; i++ ){
        if ( ((cache + i)->node_ID) == nde && ((cache + i)->blk_ID) == blk ){       //hit
            (cache + i)->time = latest_time;
            latest_time += 1;
            logMessage( LOG_INFO_LEVEL, "[cache] getSGDataBlock: blk found in cache. cache index:[%d]", i);
            hit += 1;
            return ((cache + i) -> data); 
        }
    }
    
    logMessage( LOG_INFO_LEVEL, "[cache] getSGDataBlock: blk not found in cache. cache status: [%d] lines used. ",cacheElementsCount );
    return( NULL );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : putSGDataBlock
// Description  : Get the data block from the block cache
//
// Inputs       : nde - node ID to find
//                blk - block ID to find
//                block - block to insert into cache
// Outputs      : 0 if successful, -1 if failure

int putSGDataBlock( SG_Node_ID nde, SG_Block_ID blk, char *block ) {
    bool blk_found = 0;
    SG_cache_line * temp_cache;

    for ( int i=0; i<maxElementsRecord; i++ ){ 
        if ( ((cache + i)->node_ID) == nde && ((cache + i)->blk_ID) == blk ){
            temp_cache = cache + i;
            blk_found = 1;
        }
    }
    
    if ( blk_found == 1 ){
        logMessage( LOG_INFO_LEVEL, "[Cache] putSGDataBlock: blk found and updating blk [%lu]", blk );  // updating blk
        memcpy( temp_cache->data, block, SG_BLOCK_SIZE );

    } else {
        if ( cacheElementsCount < maxElementsRecord ){
        (cache + cacheElementsCount)->time = latest_time;
        (cache + cacheElementsCount)->node_ID = nde;
        (cache + cacheElementsCount)->blk_ID = blk;
        latest_time += 1;
        memcpy( (cache + cacheElementsCount)->data, block, SG_BLOCK_SIZE );

        cacheElementsCount += 1;
        logMessage( LOG_INFO_LEVEL, "[Cache] putSGDataBlock: inserting new blk [%lu] to cache, cache status: [%d] lines used. ", blk, cacheElementsCount );

        } else if ( cacheElementsCount == maxElementsRecord ) {
            SG_cache_line * cacheLine_earliest = cache;
            for ( int i=0; i<maxElementsRecord; i++ ){  // finding earliest
                if ( ((cache + i)->time) < (cacheLine_earliest->time) ){
                    cacheLine_earliest = (cache + i);
                }
            }

            logMessage( LOG_INFO_LEVEL, "[Cache] putSGDataBlock: update oldest blk [%lu] to new blk [%lu]", cacheLine_earliest->blk_ID, blk );  //replacement policy
            cacheLine_earliest->time = latest_time;
            latest_time += 1;
            cacheLine_earliest->node_ID = nde;
            cacheLine_earliest->blk_ID = blk;
            memcpy( cacheLine_earliest->data, block, SG_BLOCK_SIZE );
            
        }
    }

    // Return successfully
    return( 0 );
}
