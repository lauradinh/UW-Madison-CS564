#include <memory.h>
#include <unistd.h>
#include <errno.h>
#include <stdlib.h>
#include <fcntl.h>
#include <iostream>
#include <stdio.h>
#include "page.h"
#include "buf.h"

/**
 * Initializes Buffer Manager and corresponding structures such as buffer hash table,
 * buffer pool, and buffer table
 * @author Laura Dinh 9080660948
 * @author Devashi Ghoshal 9081658149
 * @author Abhishek Grewal 9077664788
 */

#define ASSERT(c)                                              \
    {                                                          \
        if (!(c))                                              \
        {                                                      \
            cerr << "At line " << __LINE__ << ":" << endl      \
                 << "  ";                                      \
            cerr << "This condition should hold: " #c << endl; \
            exit(1);                                           \
        }                                                      \
    }

//----------------------------------------
// Constructor of the class BufMgr
//----------------------------------------

BufMgr::BufMgr(const int bufs)
{
    numBufs = bufs;

    bufTable = new BufDesc[bufs];
    memset(bufTable, 0, bufs * sizeof(BufDesc));
    for (int i = 0; i < bufs; i++)
    {
        bufTable[i].frameNo = i;
        bufTable[i].valid = false;
    }

    bufPool = new Page[bufs];
    memset(bufPool, 0, bufs * sizeof(Page));

    int htsize = ((((int)(bufs * 1.2)) * 2) / 2) + 1;
    hashTable = new BufHashTbl(htsize); // allocate the buffer hash table

    clockHand = bufs - 1;
}

BufMgr::~BufMgr()
{

    // flush out all unwritten pages
    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &bufTable[i];
        if (tmpbuf->valid == true && tmpbuf->dirty == true)
        {

#ifdef DEBUGBUF
            cout << "flushing page " << tmpbuf->pageNo
                 << " from frame " << i << endl;
#endif

            tmpbuf->file->writePage(tmpbuf->pageNo, &(bufPool[i]));
        }
    }

    delete[] bufTable;
    delete[] bufPool;
}

/**
 * @brief
 * allocates a free frame using clock algorithm
 * @param frame used to return free frame
 * @return const Status
 * returns BUFFEREXCEEDED if all buffer frames are pinned,
 * UNIXERR if the call to the I/O layer returned an error,
 * otherwise OK
 */

const Status BufMgr::allocBuf(int &frame)
{
    int count = 0; // counts how many frames we have checked
    while (count < 2 * (int)numBufs)
    {
        advanceClock();
        // Checks if page is valid
        if (bufTable[clockHand].valid)
        {
            // checks if reference bit is set
            if (bufTable[clockHand].refbit)
            {
                // resets reference bit and continues search
                bufTable[clockHand].refbit = false;
                count++;
                continue;
            }
            else
            {
                if (bufTable[clockHand].pinCnt == 0)
                {
                    if (bufTable[clockHand].dirty)
                    {
                        Status status;
                        // checks status of writing to disk
                        status = bufTable[clockHand].file->writePage(bufTable[clockHand].pageNo, &bufPool[clockHand]);
                        // failure to write to disk
                        if (status != OK)
                        {
                            return UNIXERR;
                        }
                        // add to total disk writes
                        bufStats.diskwrites++;
                    }
                    frame = clockHand;
                    hashTable->remove(bufTable[clockHand].file, bufTable[clockHand].pageNo);
                    bufTable[clockHand].Clear();
                    return OK;
                }
            }
        }
        else
        {
            frame = clockHand;
            return OK;
        }
        count++;
    }
    return BUFFEREXCEEDED;
}

/**
 * @brief
 * Looks up page and reads page (if necessary)
 * increment pinCnt
 * @param file file read from
 * @param PageNo page number from file
 * @param page used to return page to read
 * @return const Status returns pointer to the frame containing
 * the page via the page parameter
 */

const Status BufMgr::readPage(File *file, const int PageNo, Page *&page)
{

    // initialize variables
    Status fileStatus = OK;
    Status hashStatus = OK;
    Status insertStatus = OK;
    Status allocStatus = OK;
    int freeframe;
    int frameNo = 0;

    // use lookup to find if page is in hashTable
    hashStatus = hashTable->lookup(file, PageNo, frameNo);

    // case 1: page is NOT found in hashtable
    if (hashStatus == HASHNOTFOUND)
    {
        // use allocbuf to get a buffer frame
        allocStatus = allocBuf(freeframe);

        // if allocstatus returns any error message, return those error messages
        if (allocStatus == BUFFEREXCEEDED)
        {
            return BUFFEREXCEEDED;
        }
        else if (allocStatus == UNIXERR)
        {
            return UNIXERR;
        }
        else
        {
            // read the page from the disk
            fileStatus = file->readPage(PageNo, &bufPool[freeframe]);
            if (fileStatus != OK)
            {
                return fileStatus;
            }

            // increases the bufstats disk reads by one
            bufStats.diskreads++;

            // set the buffer pool frame's details
            bufTable[freeframe].Set(file, PageNo);

            // insert page into the hashtable
            insertStatus = hashTable->insert(file, PageNo, freeframe);
            if (insertStatus == HASHTBLERROR)
            {
                return HASHTBLERROR;
            }
            else
            {
                // set the buffer pool frame's details
                page = &bufPool[freeframe];

                // increments access to page
                bufStats.accesses++;
            }
        }
    }
    // case 2: page exists in the hashtable already
    else if (hashStatus == OK)
    {
        // set the reference bit to 1, add one to the pin count
        bufTable[frameNo].refbit = true;
        bufTable[frameNo].pinCnt++;

        // increments access to page
        page = &bufPool[frameNo];
        bufStats.accesses++;
    }
    return OK;
}

/**
 * @brief
 * if a page is found, function decrements pinCnt and sets dirty bit (if necessary)
 * @param file file where page is located
 * @param PageNo page number of page
 * @param dirty dirty bit
 * @return const Status HASHNOTFOUND if page is not in BufHashTbl,
 * PAGENOTPINNED if pin count is already 0,
 * otherwise OK
 */

const Status BufMgr::unPinPage(File *file, const int PageNo,
                               const bool dirty)
{
    int frame;
    Status status;
    status = hashTable->lookup(file, PageNo, frame);
    // checks if page is in hash table
    if (status == HASHNOTFOUND)
    {
        return status;
    }
    // Checks if page is pinned
    if (bufTable[frame].pinCnt == 0)
    {
        return PAGENOTPINNED;
    }
    // decrement pin count
    bufTable[frame].pinCnt--;
    if (dirty)
    {
        bufTable[frame].dirty = true;
    }
    return OK;
}

/**
 * @brief
 * allocates an empty page in the specified file,
 * gets buffer pool frame, and inserts page into hash tbl
 * @param file file to read from
 * @param pageNo page number of page
 * @param page used to return adress of empty frame
 * @return const Status return UNIXERR if Unix error occurs,
 * HASHTBLERROR if a hash table error occurs,
 * otherwise OK
 */

const Status BufMgr::allocPage(File *file, int &pageNo, Page *&page)
{
    file->allocatePage(pageNo);
    int freeFrame = 0;
    Status allocStatus = OK;
    Status insertStatus = OK;

    allocStatus = allocBuf(freeFrame);

    if (allocStatus == BUFFEREXCEEDED)
    {
        return BUFFEREXCEEDED;
    }
    else if (allocStatus == UNIXERR)
    {
        return UNIXERR;
    }

    page = &bufPool[freeFrame];

    bufTable[freeFrame].Set(file, pageNo);

    insertStatus = hashTable->insert(file, pageNo, freeFrame);
    if (insertStatus == HASHTBLERROR)
    {
        return HASHTBLERROR;
    }

    return OK;
}

const Status BufMgr::disposePage(File *file, const int pageNo)
{
    // see if it is in the buffer pool
    Status status = OK;
    int frameNo = 0;
    status = hashTable->lookup(file, pageNo, frameNo);
    if (status == OK)
    {
        // clear the page
        bufTable[frameNo].Clear();
    }
    status = hashTable->remove(file, pageNo);

    // deallocate it in the file
    return file->disposePage(pageNo);
}

const Status BufMgr::flushFile(const File *file)
{
    Status status;

    for (int i = 0; i < numBufs; i++)
    {
        BufDesc *tmpbuf = &(bufTable[i]);
        if (tmpbuf->valid == true && tmpbuf->file == file)
        {

            if (tmpbuf->pinCnt > 0)
                return PAGEPINNED;

            if (tmpbuf->dirty == true)
            {
#ifdef DEBUGBUF
                cout << "flushing page " << tmpbuf->pageNo
                     << " from frame " << i << endl;
#endif
                if ((status = tmpbuf->file->writePage(tmpbuf->pageNo,
                                                      &(bufPool[i]))) != OK)
                    return status;

                tmpbuf->dirty = false;
            }

            hashTable->remove(file, tmpbuf->pageNo);

            tmpbuf->file = NULL;
            tmpbuf->pageNo = -1;
            tmpbuf->valid = false;
        }

        else if (tmpbuf->valid == false && tmpbuf->file == file)
            return BADBUFFER;
    }

    return OK;
}

void BufMgr::printSelf(void)
{
    BufDesc *tmpbuf;

    cout << endl
         << "Print buffer...\n";
    for (int i = 0; i < numBufs; i++)
    {
        tmpbuf = &(bufTable[i]);
        cout << i << "\t" << (char *)(&bufPool[i])
             << "\tpinCnt: " << tmpbuf->pinCnt;

        if (tmpbuf->valid == true)
            cout << "\tvalid\n";
        cout << endl;
    };
}
