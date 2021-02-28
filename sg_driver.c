/////////////////////////////////////////////////////////////////////////////////
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

// Project Includes
#include <sg_driver.h>
#include <sg_service.h>
#include <string.h>
#include <stdbool.h>
#include <sg_cache.h>
#include <stdlib.h>

// Defines

//
// Global Data

// Driver file entry
typedef struct {
    char * name;
    int length;
    bool open;
    int position;
    SG_Node_ID node_ID[500];
    SG_Block_ID blk_ID[500];
    uint16_t blk_num;
    SgFHandle file_handle;
    } SG_File;

// seq structure
typedef struct  {
    SG_Node_ID id;
    SG_SeqNum resentSeq;
    } SG_remSeq;

// Global data
int sgDriverInitialized = 0;    // The flag indicating the driver initialized
SG_Node_ID sgLocalNodeId;   // The local node identifier
SG_SeqNum sgLocalSeqno = SG_INITIAL_SEQNO; // The local sequence number
int file_count;   // count of total files
SG_File * file_list; //global pointer to file entry
int remSeq_count;
SG_remSeq * remSeq_list; //global pointer to remSeq entry

// Driver support functions
int sgInitEndpoint( void ); // Initialize the endpoint
//
// Functions
//
// File system interface implementation

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgopen
// Description  : Open the file for for reading and writing
//
// Inputs       : path - the path/filename of the file to be read
// Outputs      : file handle if successful test, -1 if failure

SgFHandle sgopen(const char *path) {
    
    bool n = 0;

    // First check to see if we have been initialized
    if (!sgDriverInitialized) {

        // Call the endpoint initialization 
        if ( sgInitEndpoint() ) {
            logMessage( LOG_ERROR_LEVEL, "sgopen: Scatter/Gather endpoint initialization failed." );
            return( -1 );
        }
        // Set to initialized
        sgDriverInitialized = 1; 
    }
                                                                                        
    if (file_count == 0 ){
        file_list = (SG_File*) malloc(sizeof(SG_File));  //dynamic allocation
        n=1;
    }
   
    for ( SgFHandle i = 0; i<file_count; i++){
        SG_File * temp_file = file_list + i;
        if ( strcmp(temp_file->name, path) == 0 ){
            if ( temp_file->open == 0){ // if not opened, open it 
                temp_file->open = 1;
                temp_file->position = 0;
                return i;
            } else {
                return i;
            }
        }
    }

    if (n==0){
        file_list = (SG_File *) realloc(file_list, sizeof(SG_File)*(file_count+1));   //creating the first file and using realloc() to achieve dynamic allocation
        if (file_list==NULL){
            logMessage( LOG_ERROR_LEVEL, "sgopen: Scatter/Gather file pointer null." );
            return (-1);
        }
    }
    
    SG_File * new_file = file_list + (file_count);
    new_file->name = (char*) realloc(new_file->name, strlen(path));
    strcpy(new_file->name, path);
    new_file->open = 1;
    new_file->length = 0;
    new_file->position = 0;
    new_file->blk_num = 0;
    new_file->file_handle = file_count;
    file_count += 1;
    return new_file->file_handle;
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgread
// Description  : Read data from the file
//
// Inputs       : fh - file handle for the file to read from
//                buf - place to put the data
//                len - the length of the read
// Outputs      : number of bytes read, -1 if failure

int sgread(SgFHandle fh, char *buf, size_t len) {

    // Local variables
    char sendPacket[SG_DATA_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    char data[SG_BLOCK_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc_ID, rem_ID;
    SG_Block_ID blk_ID;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;

    if (fh < 0 || fh > file_count-1 || ((file_list + fh)->open) == 0 ){
        logMessage( LOG_ERROR_LEVEL, "Bad file handle or not opened. File handle:[%d]", fh );
        return (-1);
    } else {
        SG_File * target_file = file_list + fh;
        if ( (target_file->position) >= ((target_file->length)) ){
            logMessage( LOG_ERROR_LEVEL, "Bad file position. File position[%d]", target_file->position );
            return (-1);
        }

        if (len > ((target_file->length)-(target_file->position))){     // read length larger than file length
            uint16_t ori_pos = target_file->position;
            uint16_t read_pos = 0;
            uint8_t start_blk = (target_file->position)/SG_BLOCK_SIZE;
            uint8_t stop_blk = (target_file->blk_num)-1;

            for ( uint8_t i=start_blk; i<=stop_blk; i++ ){

                //setup the packet
                pktlen = SG_BASE_PACKET_SIZE;
                if ( (ret = serialize_sg_packet( sgLocalNodeId,
                                                *((target_file->node_ID)+i),
                                                *((target_file->blk_ID)+i),
                                                SG_OBTAIN_BLOCK,
                                                sgLocalSeqno++,
                                                SG_SEQNO_UNKNOWN,
                                                NULL, sendPacket, &pktlen)) != SG_PACKT_OK ) {
                    logMessage( LOG_ERROR_LEVEL, "sgread: failed serialization of packet [%d].", ret );
                    return(-1);
                }

                //send packet
                rpktlen = SG_DATA_PACKET_SIZE;
                if ( sgServicePost(sendPacket, &pktlen, recvPacket, &rpktlen) ) {
                    logMessage( LOG_ERROR_LEVEL, "sgread: failed packet post" );
                    return(-1);
                }

                //unpack
                if ( (ret = deserialize_sg_packet(&loc_ID, &rem_ID, &blk_ID, 
                                                &op, &sloc, &srem, data, recvPacket, rpktlen)) != SG_PACKT_OK ){
                    logMessage( LOG_ERROR_LEVEL, "sgread: failed deserialization of packet [%d].", ret );
                    
                    return(-1);
                }

                if ( start_blk == stop_blk ){
                    uint16_t blk_pos = (target_file->position)-(i*SG_BLOCK_SIZE);
                    memcpy ( buf, data+blk_pos, target_file->length );
                    target_file->position += target_file->length;

                } else {
                    if ( i == start_blk ){
                        uint16_t first_blk_pos = (target_file->position)-(i*SG_BLOCK_SIZE);
                        memcpy ( buf, data+first_blk_pos, SG_BLOCK_SIZE-first_blk_pos );
                        target_file->position += (SG_BLOCK_SIZE-first_blk_pos);
                        read_pos += (SG_BLOCK_SIZE-first_blk_pos);
                    } else if ( i == stop_blk ) {
                        memcpy ( buf+read_pos , data, ((target_file->length)-(i*SG_BLOCK_SIZE)));
                        target_file->position += ((target_file->length)-(i*SG_BLOCK_SIZE));
                        read_pos += ((target_file->length)-(i*SG_BLOCK_SIZE));
                    } else {
                        memcpy ( buf+read_pos , data, SG_BLOCK_SIZE );
                        target_file->position += SG_BLOCK_SIZE;
                        read_pos += SG_BLOCK_SIZE;
                    }
                }
            }
            
            return (target_file->length)-ori_pos;
        }

        if (len <= ((target_file->length)-(target_file->position))){       // read length not larger than file length
            uint16_t read_pos_s = 0;
            uint8_t start_blk_s = (target_file->position)/SG_BLOCK_SIZE;
            uint8_t stop_blk_s = (((target_file->position) + len)-1)/SG_BLOCK_SIZE;
            //logMessage( LOG_ERROR_LEVEL, "<start blk: [%d], stop blk[%d]", start_blk_s, stop_blk_s );
            SG_remSeq * current_seq;

            for ( uint8_t i=start_blk_s; i<=stop_blk_s; i++ ){
                //logMessage( LOG_ERROR_LEVEL, "<sgread: blk ID [%d], rem node ID [%d]", *((target_file->blk_ID)+i), *((target_file->node_ID)+i));
                //setup the packet

                char * cache_data = getSGDataBlock( *((target_file->node_ID)+i), *((target_file->blk_ID)+i) );

                for ( int x=0; x<remSeq_count; x++){
                    if ((remSeq_list+x)->id == *((target_file->node_ID)+i ) ){
                        current_seq = remSeq_list+x;
                    }
                }

                if ( cache_data == NULL ){
                    pktlen = SG_BASE_PACKET_SIZE;
                    if ( (ret = serialize_sg_packet( sgLocalNodeId,
                                                    *((target_file->node_ID)+i),
                                                    *((target_file->blk_ID)+i),
                                                    SG_OBTAIN_BLOCK,
                                                    sgLocalSeqno++,
                                                    (current_seq->resentSeq)+=1,
                                                    NULL, sendPacket, &pktlen)) != SG_PACKT_OK ) {
                        logMessage( LOG_ERROR_LEVEL, "sgread: failed serialization of packet [%d].", ret );
                        return(-1);
                    }

                    //send packet
                    rpktlen = SG_DATA_PACKET_SIZE;
                    if ( sgServicePost(sendPacket, &pktlen, recvPacket, &rpktlen) ) {
                        logMessage( LOG_ERROR_LEVEL, "sgread: failed packet post" );
                        return(-1);
                    }

                    //unpack
                    if ( (ret = deserialize_sg_packet(&loc_ID, &rem_ID, &blk_ID, 
                                                    &op, &sloc, &srem, data, recvPacket, rpktlen)) != SG_PACKT_OK ){
                        logMessage( LOG_ERROR_LEVEL, "sgread: failed deserialization of packet [%d].", ret );
                        return(-1);
                    }

                    if ( start_blk_s == stop_blk_s ){
                        uint16_t blk_pos = (target_file->position)-(i*SG_BLOCK_SIZE);
                        memcpy ( buf, data+blk_pos, len );
                        target_file->position += len;

                    } else {
                        if ( i == start_blk_s ){
                            uint16_t first_blk_poss = (target_file->position)-(i*SG_BLOCK_SIZE);
                            memcpy ( buf, data+first_blk_poss, SG_BLOCK_SIZE-first_blk_poss );
                            target_file->position += (SG_BLOCK_SIZE-first_blk_poss);
                            read_pos_s += (SG_BLOCK_SIZE-first_blk_poss);
                        } else if ( i == stop_blk_s ) {
                            memcpy ( buf+read_pos_s , data, ((target_file->position)-(i*SG_BLOCK_SIZE))+len );
                            target_file->position += (((target_file->position)-(i*SG_BLOCK_SIZE))+len);
                            read_pos_s += (((target_file->position)-(i*SG_BLOCK_SIZE))+len);
                        } else {
                            memcpy ( buf+read_pos_s , data, SG_BLOCK_SIZE );
                            target_file->position += SG_BLOCK_SIZE;
                            read_pos_s += SG_BLOCK_SIZE;
                        }
                    }
                    putSGDataBlock( *((target_file->node_ID)+i), *((target_file->blk_ID)+i), data );
                } else {

                    uint16_t blk_pos = (target_file->position)-(i*SG_BLOCK_SIZE);
                    memcpy ( buf, cache_data+blk_pos, len );
                    target_file->position += len;
                }
            }
            return (len);
        }
    } 
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgwrite
// Description  : write data to the file
//
// Inputs       : fh - file handle for the file to write to
//                buf - pointer to data to write
//                len - the length of the write
// Outputs      : number of bytes written if successful test, -1 if failure

int sgwrite(SgFHandle fh, char *buf, size_t len) {
    // Local variables
    char sendPacket[SG_DATA_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID new_rem_ID, loc_ID;
    SG_Block_ID new_blk_ID;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;
    uint16_t buf_offset = 0;
    char data[SG_BLOCK_SIZE];
    char temp_buf [SG_BLOCK_SIZE];
    bool f =0;

    if (fh < 0 || fh > file_count-1){
        logMessage( LOG_ERROR_LEVEL, "Bad file handle. File handle:[%d]", fh );
        return (-1);
    }

    SG_File * target_file = file_list + fh;
    
    if (target_file->open == 0){
        logMessage( LOG_ERROR_LEVEL, "sgwrite: The file is not opened. File handle:[%d]", fh );
        return (-1);
    }

    if ((target_file->position) == ((target_file->length))){    // writing at the end of the file
        //create blocks
        int remainder = len%SG_BLOCK_SIZE;
        int blk_num = len/SG_BLOCK_SIZE;
        
        if ( remainder > 0 ){           // writing at the end of the file, write half of the block (assign4)
            uint8_t target_blk = (target_file->position)/SG_BLOCK_SIZE;
            uint16_t rel_position = (target_file->position) - (target_blk*SG_BLOCK_SIZE);

            if (rel_position == 0){     //need to create new block
                //setup the packet
                pktlen = SG_DATA_PACKET_SIZE;
                if ( (ret = serialize_sg_packet( sgLocalNodeId,
                                                SG_NODE_UNKNOWN,
                                                SG_BLOCK_UNKNOWN,
                                                SG_CREATE_BLOCK,
                                                sgLocalSeqno++,
                                                SG_SEQNO_UNKNOWN,
                                                buf, sendPacket, &pktlen)) != SG_PACKT_OK ) {
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: failed serialization of packet [%d].", ret );
                    return(-1);
                }
                //send packet
                rpktlen = SG_BASE_PACKET_SIZE;
                if ( sgServicePost(sendPacket, &pktlen, recvPacket, &rpktlen) ) {
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: failed packet post" );
                    return(-1);
                }
                //unpack
                if ( (ret = deserialize_sg_packet(&loc_ID, &new_rem_ID, &new_blk_ID, 
                                                &op, &sloc, &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ){
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: failed deserialization of packet [%d].", ret );
                    return(-1);
                }
                //Check assigned block and node ID
                if ( new_blk_ID == SG_BLOCK_UNKNOWN ){
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: bad new remote block ID [%d].", new_blk_ID );
                    return(-1);
                }
                if ( new_rem_ID == SG_NODE_UNKNOWN ){
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: bad new remote node ID [%d].", new_rem_ID );
                    return(-1);
                } else {
                    if ( remSeq_count == 0 ){
                        remSeq_list = (SG_remSeq *) malloc( sizeof(SG_remSeq) );
                        (remSeq_list+remSeq_count)->id = new_rem_ID;
                        (remSeq_list+remSeq_count)->resentSeq = srem;
                        remSeq_count += 1;
                    } else {
                        for ( int i=0; i<remSeq_count; i++){
                            if ((remSeq_list+i)->id == new_rem_ID ){
                                (remSeq_list+i)->resentSeq = srem;
                                f=1;
                            }
                        }
                        if ( f==0 ){
                            remSeq_list = (SG_remSeq *) realloc( remSeq_list, sizeof(SG_remSeq)*(remSeq_count+1) );
                            (remSeq_list+remSeq_count)->id = new_rem_ID;
                            (remSeq_list+remSeq_count)->resentSeq = srem;
                            remSeq_count += 1;
                        }
                    }
                }
                 
                putSGDataBlock( new_rem_ID, new_blk_ID, buf );      

                SG_Block_ID * blk_ID_ptr = (file_list+fh)->blk_ID;
                SG_Node_ID * node_ID_ptr = (file_list+fh)->node_ID;
                uint16_t temp_blk_num = (file_list+fh)->blk_num;
                *(blk_ID_ptr+temp_blk_num) = new_blk_ID;
                *(node_ID_ptr+temp_blk_num) = new_rem_ID;
                (file_list+fh)-> length +=len;
                (file_list+fh)-> position = (file_list+fh)->length;
                (file_list+fh)->blk_num = (file_list+fh)->blk_num+1;

            } else if ( rel_position != 0 ) {     //  writing at the end of the file, updating the blk
                char * temp_g = getSGDataBlock( *((target_file->node_ID)+target_blk), *((target_file->blk_ID)+target_blk) );
                SG_remSeq * current_seq;

                for ( int i=0; i<remSeq_count; i++){;

                        if ( (remSeq_list+i)->id == (*((target_file->node_ID)+target_blk )) ){
                            current_seq = remSeq_list+i;
                          \
                        }
                    }

                if ( temp_g != NULL ){
                    memcpy( data, temp_g, SG_BLOCK_SIZE );
                } else {
                    
                    pktlen = SG_BASE_PACKET_SIZE;
                    if ( (ret = serialize_sg_packet( sgLocalNodeId,
                                                    *((target_file->node_ID)+target_blk),
                                                    *((target_file->blk_ID)+target_blk),
                                                    SG_OBTAIN_BLOCK,
                                                    sgLocalSeqno++,
                                                    (current_seq->resentSeq)+=1,
                                                    NULL, sendPacket, &pktlen)) != SG_PACKT_OK ) {
                        logMessage( LOG_ERROR_LEVEL, "sgwrite: failed serialization of packet [%d].", ret );
                        return(-1);
                    }
                    //send packet
                    rpktlen = SG_DATA_PACKET_SIZE;
                    if ( sgServicePost(sendPacket, &pktlen, recvPacket, &rpktlen) ) {
                        logMessage( LOG_ERROR_LEVEL, "sgwrite: failed packet post" );
                        return(-1);
                    }
                    //unpack
                    if ( (ret = deserialize_sg_packet(&loc_ID, &new_rem_ID, &new_blk_ID, 
                                                    &op, &sloc, &srem, data, recvPacket, rpktlen)) != SG_PACKT_OK ){
                        logMessage( LOG_ERROR_LEVEL, "sgwrite: failed deserialization of packet [%d].", ret );
                        return(-1);
                    }
                    putSGDataBlock( *((target_file->node_ID)+target_blk), *((target_file->blk_ID)+target_blk), data );
                }
                
                if ( rel_position == 256 ){
                    memcpy(temp_buf, data, 256);
                    memcpy(temp_buf+256, buf, 256);
                } else if ( rel_position == 512 ){
                    memcpy(temp_buf, data, 512);
                    memcpy(temp_buf+512, buf, 256);
                } else if ( rel_position == 768 ) {
                    memcpy(temp_buf, data, 768);
                    memcpy(temp_buf+768, buf, 256);
                }

                pktlen = SG_DATA_PACKET_SIZE;
                if ( (ret = serialize_sg_packet( sgLocalNodeId,
                                                *((target_file->node_ID)+target_blk),
                                                *((target_file->blk_ID)+target_blk),
                                                SG_UPDATE_BLOCK,
                                                sgLocalSeqno++,
                                                (current_seq->resentSeq)+=1,
                                                temp_buf, sendPacket, &pktlen)) != SG_PACKT_OK ) {
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: failed serialization of packet [%d].", ret );
                    return(-1);
                }
                //send packet
                rpktlen = SG_BASE_PACKET_SIZE;
                if ( sgServicePost(sendPacket, &pktlen, recvPacket, &rpktlen) ) {
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: failed packet post" );
                    return(-1);
                }
                //unpack
                if ( (ret = deserialize_sg_packet(&loc_ID, &new_rem_ID, &new_blk_ID, 
                                                &op, &sloc, &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ){
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: failed deserialization of packet [%d].", ret );
                    return(-1);
                }
                //Check assigned block and node ID
                if ( new_blk_ID == SG_BLOCK_UNKNOWN ){
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: bad new remote block ID [%d].", new_blk_ID );
                    return(-1);
                }
                if ( new_rem_ID == SG_NODE_UNKNOWN ){
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: bad new remote node ID [%d].", new_rem_ID );
                    return(-1);
                }

                if ( getSGDataBlock( *((target_file->node_ID)+target_blk), *((target_file->blk_ID)+target_blk) ) != NULL ){
                    putSGDataBlock( *((target_file->node_ID)+target_blk), *((target_file->blk_ID)+target_blk), temp_buf );
                }

                (file_list+fh)-> length +=len;
                (file_list+fh)-> position = (file_list+fh)->length;
            }
                
        } else {     
            for (int i=0; i<blk_num; i++){  // wirte the whole block (assign3) and assuming writing multiple blocks a time
                //setup the packet
                pktlen = SG_DATA_PACKET_SIZE;
                if ( (ret = serialize_sg_packet( sgLocalNodeId,
                                                SG_NODE_UNKNOWN,
                                                SG_BLOCK_UNKNOWN,
                                                SG_CREATE_BLOCK,
                                                sgLocalSeqno++,
                                                SG_SEQNO_UNKNOWN,
                                                buf+buf_offset, sendPacket, &pktlen)) != SG_PACKT_OK ) {
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: failed serialization of packet [%d].", ret );
                    return(-1);
                }
                //send packet
                rpktlen = SG_BASE_PACKET_SIZE;
                if ( sgServicePost(sendPacket, &pktlen, recvPacket, &rpktlen) ) {
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: failed packet post" );
                    return(-1);
                }
                //unpack
                if ( (ret = deserialize_sg_packet(&loc_ID, &new_rem_ID, &new_blk_ID, 
                                                &op, &sloc, &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ){
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: failed deserialization of packet [%d].", ret );
                    return(-1);
                }
                //Check assigned block and node ID
                if ( new_blk_ID == SG_BLOCK_UNKNOWN ){
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: bad new remote block ID [%d].", new_blk_ID );
                    return(-1);
                }
                if ( new_rem_ID == SG_NODE_UNKNOWN ){
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: bad new remote node ID [%d].", new_rem_ID );
                    return(-1);
                }

                SG_Block_ID * blk_ID_ptr_s = (file_list+fh)->blk_ID;
                SG_Node_ID * node_ID_ptr_s = (file_list+fh)->node_ID;

                uint16_t temp_blk_num_s = (file_list+fh)->blk_num;

                *(blk_ID_ptr_s+temp_blk_num_s) = new_blk_ID;
                *(node_ID_ptr_s+temp_blk_num_s) = new_rem_ID;
                //logMessage ( LOG_ERROR_LEVEL, "sgwrite: new blk_num is: [%d], number is [%d], new rem ID is [%d] ", (file_list+fh)->blk_num, *(blk_ID_ptr_s+temp_blk_num_s), new_rem_ID);
                (file_list+fh)->length +=len;
                (file_list+fh)->position = (file_list+fh)->length;
                (file_list+fh)->blk_num +=1;

                buf_offset += SG_BLOCK_SIZE;
            }
        }
    } else if ((target_file->position) < (target_file->length)) {   // writing to the middle of the file at 0 or 256 or 512 or 768
        uint8_t target_blk_m = (target_file->position)/SG_BLOCK_SIZE;
        uint16_t rel_position = (target_file->position) - (target_blk_m*SG_BLOCK_SIZE);
        SG_remSeq * current_seq_m;

        char * temp_m = getSGDataBlock( *((target_file->node_ID)+target_blk_m), *((target_file->blk_ID)+target_blk_m) );

             for ( int i=0; i<remSeq_count; i++){
                    if ((remSeq_list+i)->id == *((target_file->node_ID)+target_blk_m ) ){
                        current_seq_m = remSeq_list+i;
                    }
                }

            if ( temp_m != NULL ){
                memcpy( data, temp_m, SG_BLOCK_SIZE );

            } else {
                
                pktlen = SG_BASE_PACKET_SIZE;
                if ( (ret = serialize_sg_packet( sgLocalNodeId,
                                                *((target_file->node_ID)+target_blk_m),
                                                *((target_file->blk_ID)+target_blk_m),
                                                SG_OBTAIN_BLOCK,
                                                sgLocalSeqno++,
                                                (current_seq_m->resentSeq)+=1,
                                                NULL, sendPacket, &pktlen)) != SG_PACKT_OK ) {
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: failed serialization of packet [%d].", ret );
                    return(-1);
                }
                //send packet
                rpktlen = SG_DATA_PACKET_SIZE;
                if ( sgServicePost(sendPacket, &pktlen, recvPacket, &rpktlen) ) {
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: failed packet post" );
                    return(-1);
                }
                //unpack
                if ( (ret = deserialize_sg_packet(&loc_ID, &new_rem_ID, &new_blk_ID, 
                                                &op, &sloc, &srem, data, recvPacket, rpktlen)) != SG_PACKT_OK ){
                    logMessage( LOG_ERROR_LEVEL, "sgwrite: failed deserialization of packet [%d].", ret );
                    return(-1);
                }
               putSGDataBlock( *((target_file->node_ID)+target_blk_m), *((target_file->blk_ID)+target_blk_m), data );
            }


        if ( rel_position == 0 ){
            memcpy(temp_buf, buf, 256);
            memcpy(temp_buf+256, data+256, 768);

        } else if ( rel_position == 256 ){
            memcpy(temp_buf, data, 256);
            memcpy(temp_buf+256, buf, 256);
            memcpy(temp_buf+512, data+512, 512);

        } else if ( rel_position == 512 ) {
            memcpy(temp_buf, data, 512);
            memcpy(temp_buf+512, buf, 256);
            memcpy(temp_buf+768, data+768, 256);

        } else if ( rel_position == 768 ) {
            memcpy(temp_buf, data, 768);
            memcpy(temp_buf+768, buf, 256);
        }

        pktlen = SG_DATA_PACKET_SIZE;
        if ( (ret = serialize_sg_packet( sgLocalNodeId,
                                        *((target_file->node_ID)+target_blk_m),
                                        *((target_file->blk_ID)+target_blk_m),
                                        SG_UPDATE_BLOCK,
                                        sgLocalSeqno++,
                                        (current_seq_m->resentSeq)+=1,
                                        temp_buf, sendPacket, &pktlen)) != SG_PACKT_OK ) {
            logMessage( LOG_ERROR_LEVEL, "sgwrite: failed serialization of packet [%d].", ret );
            return(-1);
        }
        //send packet
        rpktlen = SG_BASE_PACKET_SIZE;
        if ( sgServicePost(sendPacket, &pktlen, recvPacket, &rpktlen) ) {
            logMessage( LOG_ERROR_LEVEL, "sgwrite: failed packet post" );
            return(-1);
        }
        //unpack
        if ( (ret = deserialize_sg_packet(&loc_ID, &new_rem_ID, &new_blk_ID, 
                                        &op, &sloc, &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ){
            logMessage( LOG_ERROR_LEVEL, "sgwrite: failed deserialization of packet [%d].", ret );
            return(-1);
        }
        //Check assigned block and node ID
        if ( new_blk_ID == SG_BLOCK_UNKNOWN ){
            logMessage( LOG_ERROR_LEVEL, "sgwrite: bad new remote block ID [%d].", new_blk_ID );
            return(-1);
        }
        if ( new_rem_ID == SG_NODE_UNKNOWN ){
            logMessage( LOG_ERROR_LEVEL, "sgwrite: bad new remote node ID [%d].", new_rem_ID );
            return(-1);
        }
        (target_file->position) += len;

        if ( getSGDataBlock( *((target_file->node_ID)+target_blk_m), *((target_file->blk_ID)+target_blk_m) ) != NULL ){
            putSGDataBlock( *((target_file->node_ID)+target_blk_m), *((target_file->blk_ID)+target_blk_m), temp_buf );
        }

    }
    // Log the write, return bytes written
    return( len );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgseek
// Description  : Seek to a specific place in the file
//
// Inputs       : fh - the file handle of the file to seek in
//                off - offset within the file to seek to
// Outputs      : new position if successful, -1 if failure

int sgseek(SgFHandle fh, size_t off) {

    if (fh < 0 || fh > file_count-1){
        logMessage( LOG_ERROR_LEVEL, "sgseek: Bad file handle. File handle:[%d]", fh );
        return (-1);
    }

    if ( off > (file_list+fh)->length ){
        logMessage( LOG_ERROR_LEVEL, "sgseek: Bad offset. Length: [%d], offset: [%d]", (file_list+fh)->length, off );
        return(-1);
    }

    SG_File * target_file = file_list + fh;
    target_file->position = off;

    //logMessage( LOG_ERROR_LEVEL, "seeked to:[%d], File handle:[%d]", off, fh );
    // Return new position
    return( off );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgclose
// Description  : Close the file
//
// Inputs       : fh - the file handle of the file to close
// Outputs      : 0 if successful test, -1 if failure

int sgclose(SgFHandle fh) {

    if (fh < 0 || fh > file_count-1){
        logMessage( LOG_ERROR_LEVEL, "sgclose: Bad file handle. File handle:[%d]", fh );
        return (-1);
    }

    if ( (file_list+fh)->open == 0 ){
        logMessage( LOG_ERROR_LEVEL, "sgclose: Bad file handle, file [%d] was not opened", fh);
        return(-1);
    }

    (file_list+fh)->open = 0;
    // Return successfully
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgshutdown
// Description  : Shut down the filesystem
//
// Inputs       : none
// Outputs      : 0 if successful test, -1 if failure

int sgshutdown(void) {
    // Local variables
    char sendPacket[SG_DATA_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID rem_ID, loc_ID;
    SG_Block_ID blk_ID;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;

    //packing
    pktlen = SG_DATA_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( sgLocalNodeId,
                                    SG_NODE_UNKNOWN,
                                    SG_BLOCK_UNKNOWN,
                                    SG_STOP_ENDPOINT,
                                    sgLocalSeqno++,
                                    SG_SEQNO_UNKNOWN,
                                    NULL, sendPacket, &pktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgshutdown: failed serialization of packet [%d].", ret );
        return(-1);
    }

    //send packet
    rpktlen = SG_DATA_PACKET_SIZE;
    if ( sgServicePost(sendPacket, &pktlen, recvPacket, &rpktlen) ) {
        logMessage( LOG_ERROR_LEVEL, "sgshutdown: failed packet post" );
        return(-1);
    }

    //unpack
    if ( (ret = deserialize_sg_packet(&loc_ID, &rem_ID, &blk_ID, 
                                    &op, &sloc, &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ){
        logMessage( LOG_ERROR_LEVEL, "sgshutdown: failed deserialization of packet [%d].", ret );
        return(-1);
    }

    if ( closeSGCache() == 0 ){
        logMessage( LOG_INFO_LEVEL, "Shut down SG cache." );
    }

    // Log, return successfully
    logMessage( LOG_INFO_LEVEL, "Shut down Scatter/Gather driver." );
    logMessage( LOG_INFO_LEVEL, "Freeing pointers..." );
    free(file_list);
    free(remSeq_list);
    file_list = NULL;
    remSeq_list = NULL;
    
    return( 0 );
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : serialize_sg_packet
// Description  : Serialize a ScatterGather packet (create packet)
//
// Inputs       : loc - the local node identifier
//                rem - the remote node identifier
//                blk - the block identifier
//                op - the operation performed/to be performed on block
//                sseq - the sender sequence number
//                rseq - the receiver sequence number
//                data - the data block (of size SG_BLOCK_SIZE) or NULL
//                packet - the buffer to place the data
//                plen - the packet length (int bytes)
// Outputs      : 0 if successfully created, -1 if failure

SG_Packet_Status serialize_sg_packet(SG_Node_ID loc, SG_Node_ID rem, SG_Block_ID blk,
                                     SG_System_OP op, SG_SeqNum sseq, SG_SeqNum rseq, char *data,
                                     char *packet, size_t *plen) {

            uint32_t magic_copy = SG_MAGIC_VALUE;
            SG_Node_ID loc_copy = loc;
            SG_Node_ID rem_copy = rem;
            SG_Block_ID blk_copy = blk;
            SG_System_OP op_copy = op;
            SG_SeqNum sseq_copy = sseq;
            SG_SeqNum rseq_copy = rseq;
            uint8_t data_indicator;

            if ( packet == NULL){
                return ( SG_PACKT_PDATA_BAD );
            }

            if ( loc == 0 ){
                return ( SG_PACKT_LOCID_BAD );
            }
            if ( rem == 0 ){
                return ( SG_PACKT_REMID_BAD );
            }
            if ( sseq == 0 ){
                return ( SG_PACKT_SNDSQ_BAD );
            }
            if ( rseq == 0 ){
                return ( SG_PACKT_RCVSQ_BAD );
            }
            if ( blk == 0 ) {
                return ( SG_PACKT_BLKID_BAD );
            }
            if ( op > 6 || op < 0) {
                return ( SG_PACKT_OPERN_BAD );
            }

            if ( data == NULL ){
                *plen = SG_BASE_PACKET_SIZE;
                data_indicator = 0;

            } else {
                *plen = SG_DATA_PACKET_SIZE;
                data_indicator = 1;
            }

            memcpy(packet, &magic_copy, sizeof(uint32_t));
            memcpy(packet+4, &loc_copy, sizeof(SG_Node_ID));
            memcpy(packet+12, &rem_copy, sizeof(SG_Node_ID));
            memcpy(packet+20, &blk_copy, sizeof(SG_Block_ID));
            memcpy(packet+28, &op_copy, sizeof(SG_System_OP));
            memcpy(packet+32, &sseq_copy, sizeof(SG_SeqNum));
            memcpy(packet+34, &rseq_copy, sizeof(SG_SeqNum));
            memcpy(packet+36, &data_indicator, 1);

            if ( data != NULL ){
                memcpy(packet+37, data, SG_BLOCK_SIZE);
                memcpy(packet+1061, &magic_copy, sizeof(uint32_t));

            } else {
                memcpy(packet+37, &magic_copy, sizeof(uint32_t));
            }

    // Return the system function return value
    return (SG_PACKT_OK);
    
}

////////////////////////////////////////////////////////////////////////////////
//
// Function     : deserialize_sg_packet
// Description  : De-serialize a ScatterGather packet (unpack packet)
//
// Inputs       : loc - the local node identifier
//                rem - the remote node identifier
//                blk - the block identifier
//                op - the operation performed/to be performed on block
//                sseq - the sender sequence number
//                rseq - the receiver sequence number
//                data - the data block (of size SG_BLOCK_SIZE) or NULL
//                packet - the buffer to place the data
//                plen - the packet length (int bytes)
// Outputs      : 0 if successfully created, -1 if failure

SG_Packet_Status deserialize_sg_packet(SG_Node_ID *loc, SG_Node_ID *rem, SG_Block_ID *blk,
                                       SG_System_OP *op, SG_SeqNum *sseq, SG_SeqNum *rseq, char *data,
                                       char *packet, size_t plen) {

            SG_Node_ID loc_value =0;
            SG_Node_ID *loc_ptr = &loc_value;
            memcpy( loc_ptr, packet+4, sizeof(SG_Node_ID));

            SG_Node_ID rem_value =0;
            SG_Node_ID *rem_ptr = &rem_value;
            memcpy( rem_ptr, packet+12, sizeof(SG_Node_ID));

            SG_Block_ID blk_value =0;
            SG_Block_ID *blk_ptr = &blk_value;
            memcpy( blk_ptr, packet+20, sizeof(SG_Block_ID));

            SG_System_OP op_value =0;
            SG_System_OP *op_ptr = &op_value;
            memcpy( op_ptr, packet+28, sizeof(SG_System_OP));

            SG_SeqNum sseq_value =0;
            SG_SeqNum *sseq_ptr = &sseq_value;
            memcpy( sseq_ptr, packet+32, sizeof(SG_SeqNum));

            SG_SeqNum rseq_value =0;
            SG_SeqNum *rseq_ptr = &rseq_value;
            memcpy( rseq_ptr, packet+34, sizeof(SG_SeqNum));

            if ( packet == NULL){
                return ( SG_PACKT_PDATA_BAD );
            }

            if ( loc_value == 0 ){
                return ( SG_PACKT_LOCID_BAD );
            }
            if ( rem_value == 0 ){
                return ( SG_PACKT_REMID_BAD );
            }
            if ( blk_value == 0 ){
                return ( SG_PACKT_BLKID_BAD );
            }
            if ( op_value > 6 || op_value < 0 ){
                return ( SG_PACKT_OPERN_BAD );
            }
            if ( sseq_value == 0 ) {
                return ( SG_PACKT_SNDSQ_BAD );
            }
            if ( rseq_value == 0 ) {
                return ( SG_PACKT_RCVSQ_BAD );
            }

            if ( plen == SG_BASE_PACKET_SIZE ){
                memcpy( loc, packet+4, sizeof(SG_Node_ID));
                memcpy( rem, packet+12, sizeof(SG_Node_ID));
                memcpy( blk, packet+20, sizeof(SG_Block_ID));
                memcpy( op, packet+28, sizeof(SG_System_OP));
                memcpy( sseq, packet+32, sizeof(SG_SeqNum));
                memcpy( rseq, packet+34, sizeof(SG_SeqNum));
            } else {
                if ( data == NULL){
                    return ( SG_PACKT_BLKDT_BAD );
                }
                memcpy( loc, packet+4, sizeof(SG_Node_ID));
                memcpy( rem, packet+12, sizeof(SG_Node_ID));
                memcpy( blk, packet+20, sizeof(SG_Block_ID));
                memcpy( op, packet+28, sizeof(SG_System_OP));
                memcpy( sseq, packet+32, sizeof(SG_SeqNum));
                memcpy( rseq, packet+34, sizeof(SG_SeqNum));
                memcpy( data, packet+37, SG_BLOCK_SIZE);
            }
            
             // Return the system function return value
        return (SG_PACKT_OK);
    }
//
// Driver support functions

////////////////////////////////////////////////////////////////////////////////
//
// Function     : sgInitEndpoint
// Description  : Initialize the endpoint
//
// Inputs       : none
// Outputs      : 0 if successfull, -1 if failure

int sgInitEndpoint( void ) {

    // Local variables
    char initPacket[SG_BASE_PACKET_SIZE], recvPacket[SG_BASE_PACKET_SIZE];
    size_t pktlen, rpktlen;
    SG_Node_ID loc, rem;
    SG_Block_ID blkid;
    SG_SeqNum sloc, srem;
    SG_System_OP op;
    SG_Packet_Status ret;

    // Local and do some initial setup
    logMessage( LOG_INFO_LEVEL, "Initializing local endpoint ..." );
    sgLocalSeqno = SG_INITIAL_SEQNO;

    // Setup the packet
    pktlen = SG_BASE_PACKET_SIZE;
    if ( (ret = serialize_sg_packet( SG_NODE_UNKNOWN, // Local ID nodeID
                                    SG_NODE_UNKNOWN,   // Remote ID nodeID
                                    SG_BLOCK_UNKNOWN,  // Block ID
                                    SG_INIT_ENDPOINT,  // Operation
                                    sgLocalSeqno++,    // Sender sequence number
                                    SG_SEQNO_UNKNOWN,  // Receiver sequence number
                                    NULL, initPacket, &pktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed serialization of packet [%d].", ret );
        return( -1 );
    }

    // Send the packet
    rpktlen = SG_BASE_PACKET_SIZE;
    if ( sgServicePost(initPacket, &pktlen, recvPacket, &rpktlen) ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed packet post" );
        return( -1 );
    }

    // Unpack the recieived data
    if ( (ret = deserialize_sg_packet(&loc, &rem, &blkid, &op, &sloc, 
                                    &srem, NULL, recvPacket, rpktlen)) != SG_PACKT_OK ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: failed deserialization of packet [%d]", ret );
        return( -1 );
    }

    // Sanity check the return value
    if ( loc == SG_NODE_UNKNOWN ) {
        logMessage( LOG_ERROR_LEVEL, "sgInitEndpoint: bad local ID returned [%ul]", loc );
        return( -1 );
    }

    // Set the local node ID, log and return successfully
    sgLocalNodeId = loc;

    logMessage( LOG_INFO_LEVEL, "Completed initialization of node (local node ID %lu", sgLocalNodeId );

    if ( initSGCache( SG_MAX_CACHE_ELEMENTS ) == 0 ){
        logMessage( LOG_INFO_LEVEL, "Completed initialization of cache" );
    }
    return( 0 );
}
