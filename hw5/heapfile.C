#include "heapfile.h"
#include "error.h"

/**
 *
 * @author Laura Dinh 9080660948
 * @author Devashi Ghoshal 9081658149
 * @author Abhishek Grewal 9077664788
 */

/**
 * Creates an empty heap file
 * If file doesn't exist, db level file,
 * then allocate an empty page
 * Use the page pointer returned and cast it to FileHdrPage
 * Initialize values in the header page
 * Allocate another page for the first data page
 * Intialize values page contents and store page number of the data page
 * in firstPage and lastPage of the FildHdrPage
 *
 * @param fileName - db file level file name
 * @return Status - returns OK if works as expected
 */
const Status createHeapFile(const string fileName)
{
    File *file;
    Status status;
    FileHdrPage *hdrPage;
    int hdrPageNo;
    int newPageNo;
    Page *newPage;

    // try to open the file. This should return an error
    status = db.openFile(fileName, file);
    if (status != OK)
    {
        // file doesn't exist

        // create new file
        status = db.createFile(fileName);
        if (status == OK)
        {
            // open file
            status = db.openFile(fileName, file);
            if (status == OK)
            {
                // allocate page for file
                status = bufMgr->allocPage(file, hdrPageNo, newPage);

                if (status == OK)
                {
                    // cast newly allocated page to hdrPage
                    hdrPage = (FileHdrPage *)newPage;

                    // initialize values in the header page
                    strncpy(hdrPage->fileName, fileName.c_str(), MAXNAMESIZE);

                    // allocate page for data page
                    status = bufMgr->allocPage(file, newPageNo, newPage);

                    if (status == OK)
                    {
                        // initialize page contents
                        newPage->init(newPageNo);

                        // store page number in hdrPage
                        hdrPage->firstPage = newPageNo;
                        hdrPage->lastPage = newPageNo;
                        hdrPage->pageCnt = 1;
                        hdrPage->recCnt = 0;

                        // unpin hdrPage and mark dirty
                        status = bufMgr->unPinPage(file, hdrPageNo, true);
                        if (status != OK)
                        {
                            return status;
                        }

                        // unpin data page and mark dirty
                        status = bufMgr->unPinPage(file, newPageNo, true);
                        if (status != OK)
                        {
                            return status;
                        }
                    }
                }
            }
        }
        status = bufMgr->flushFile(file);
        status = db.closeFile(file);
        return status;
    }
    return (FILEEXISTS);
}

// routine to destroy a heapfile
const Status destroyHeapFile(const string fileName)
{
    return (db.destroyFile(fileName));
}

// constructor opens the underlying file
/**
 * Opens file, reads and pins the header page for the file in the buffer pool
 * Intialize the private data members: headerPage, headerPageNo, and hdrDirtyFlag
 * Read and pin the first page of the file into the buffer pool
 * Initialize curPage, curPageNo, and curDirtyFlag
 * Set curRec to NULLRID
 */
// constructor opens the underlying file
HeapFile::HeapFile(const string &fileName, Status &returnStatus)
{
    Status status;
    Page *pagePtr;

    cout << "opening file " << fileName << endl;

    // open the file and read in the header page and the first data page
    if ((status = db.openFile(fileName, filePtr)) == OK)
    {

        status = filePtr->getFirstPage(headerPageNo);

        if (status != OK)
        {
            returnStatus = status;
        }

        // read page contents into pagePtr
        status = bufMgr->readPage(filePtr, headerPageNo, pagePtr);
        if (status != OK)
        {
            returnStatus = status;
        }

        // initialize headerPage and set dirty flag to false
        headerPage = (FileHdrPage *)pagePtr;
        hdrDirtyFlag = false;

        // set curPageNo and get contents of data page
        curPageNo = headerPage->firstPage;
        status = bufMgr->readPage(filePtr, curPageNo, curPage);

        if (status != OK)
        {
            returnStatus = status;
        }

        curDirtyFlag = false;
        curRec = NULLRID;
        returnStatus = status;
    }
    else
    {
        cerr << "open of heap file failed\n";
        returnStatus = status;
    }
    return;
}

// the destructor closes the file
HeapFile::~HeapFile()
{
    Status status;
    cout << "invoking heapfile destructor on file " << headerPage->fileName << endl;

    // see if there is a pinned data page. If so, unpin it
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        if (status != OK)
            cerr << "error in unpin of date page\n";
    }

    // unpin the header page
    status = bufMgr->unPinPage(filePtr, headerPageNo, hdrDirtyFlag);
    if (status != OK)
        cerr << "error in unpin of header page\n";

    // status = bufMgr->flushFile(filePtr);  // make sure all pages of the file are flushed to disk
    // if (status != OK) cerr << "error in flushFile call\n";
    // before close the file
    status = db.closeFile(filePtr);
    if (status != OK)
    {
        cerr << "error in closefile call\n";
        Error e;
        e.print(status);
    }
}

// Return number of records in heap file

const int HeapFile::getRecCnt() const
{
    return headerPage->recCnt;
}

/**
 * retrieve an arbitrary record from a file.
 * if record is not on the currently pinned page, the current page
 * is unpinned and the required page is read into the buffer pool
 * and pinned.
 * @param rid - record ID
 * @param rec - ponter to the record
 * @return returns a pointer to the record via the rec parameter
 *
 */
const Status HeapFile::getRecord(const RID &rid, Record &rec)
{
    Status status;

    // cout<< "getRecord. record (" << rid.pageNo << "." << rid.slotNo << ")" << endl;

    // if curPage is NULL, get the correct page
    if (curPage == NULL)
    {
        status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
        if (status != OK)
        {
            return status;
        }
        curPageNo = rid.pageNo;
        curDirtyFlag = false;
        curRec = rid;
        status = curPage->getRecord(rid, rec);
        return status;
    }
    // if curPage contains a page
    else
    {
        // check if it's the correct page
        if (rid.pageNo == curPageNo)
        {
            status = curPage->getRecord(rid, rec);
            curRec = rid;
            return OK;
        }
        // remove incorrect page and pin correct page
        else
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
            {
                curPage = NULL;
                curPageNo = -1;
                curDirtyFlag = false;
                return status;
            }
            status = bufMgr->readPage(filePtr, rid.pageNo, curPage);
            if (status != OK)
            {
                return status;
            }
            curPageNo = rid.pageNo;
            curDirtyFlag = false;
            curRec = rid;
            status = curPage->getRecord(rid, rec);
            return status;
        }
    }
}

HeapFileScan::HeapFileScan(const string &name,
                           Status &status) : HeapFile(name, status)
{
    filter = NULL;
}

const Status HeapFileScan::startScan(const int offset_,
                                     const int length_,
                                     const Datatype type_,
                                     const char *filter_,
                                     const Operator op_)
{
    if (!filter_)
    { // no filtering requested
        filter = NULL;
        return OK;
    }

    if ((offset_ < 0 || length_ < 1) ||
        (type_ != STRING && type_ != INTEGER && type_ != FLOAT) ||
        (type_ == INTEGER && length_ != sizeof(int) || type_ == FLOAT && length_ != sizeof(float)) ||
        (op_ != LT && op_ != LTE && op_ != EQ && op_ != GTE && op_ != GT && op_ != NE))
    {
        return BADSCANPARM;
    }

    offset = offset_;
    length = length_;
    type = type_;
    filter = filter_;
    op = op_;

    return OK;
}

const Status HeapFileScan::endScan()
{
    Status status;
    // generally must unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        curPage = NULL;
        curPageNo = 0;
        curDirtyFlag = false;
        return status;
    }
    return OK;
}

HeapFileScan::~HeapFileScan()
{
    endScan();
}

const Status HeapFileScan::markScan()
{
    // make a snapshot of the state of the scan
    markedPageNo = curPageNo;
    markedRec = curRec;
    return OK;
}

const Status HeapFileScan::resetScan()
{
    Status status;
    if (markedPageNo != curPageNo)
    {
        if (curPage != NULL)
        {
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            if (status != OK)
                return status;
        }
        // restore curPageNo and curRec values
        curPageNo = markedPageNo;
        curRec = markedRec;
        // then read the page
        status = bufMgr->readPage(filePtr, curPageNo, curPage);
        if (status != OK)
            return status;
        curDirtyFlag = false; // it will be clean
    }
    else
        curRec = markedRec;
    return OK;
}

/**
 * Returns the RID of the next record that satisfies the scan predicate
 * @param outRid - RID of next record
 * @return - returns OK if no errors occured. Otherwise, return the error
 * code of the first error occurred
 */
const Status HeapFileScan::scanNext(RID &outRid)
{
    Status status = OK;
    RID nextRid;
    RID tmpRid;
    int nextPageNo;
    Record rec;
    bool matchFound = false;

    if (curPageNo == -1)
    {
        return FILEEOF;
    }

    // finds if there is a valid current page
    if (curPage == NULL)
    {
        // no current page found, first page becomes current page
        curPageNo = headerPage->firstPage;
        if (curPageNo == -1)
        {
            return FILEEOF;
        }
        status = bufMgr->readPage(filePtr, curPageNo, curPage);

        if (status != OK)
        {
            return status;
        }
        else
        {
            // reintialize for new current page
            curDirtyFlag = false;
            curRec = NULLRID;

            // gets first record of current page
            status = curPage->firstRecord(tmpRid);
            curRec = tmpRid;

            // checks if there are records on the page
            if (status == NORECORDS)
            {
                // if there are no records, the page is unpinned
                status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
                if (status != OK)
                {
                    return status;
                }
                curPage = NULL;
                curPageNo = -1;
                curDirtyFlag = false;
                // no more records
                return FILEEOF;
            }
            // reads in first record
            status = curPage->getRecord(tmpRid, rec);
            if (status != OK)
            {
                return status;
            }
            if (matchRec(rec))
            {
                outRid = tmpRid;
                matchFound = true;
            }
        }
    }

    // if the first record wasn't a match, loop
    while (!matchFound)
    {
        // gets next record
        status = curPage->nextRecord(curRec, nextRid);

        // if valid, next record becomes new current record
        if (status == OK)
        {
            curRec = nextRid;
        }
        while (status != OK)
        {
            curPage->getNextPage(nextPageNo);
            if (nextPageNo == -1)
            {
                return FILEEOF;
            }
            // if there is a nextPage, unpin the current page
            status = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
            curPage = NULL;
            curPageNo = -1;
            curDirtyFlag = false;
            if (status != OK)
            {
                return status;
            }
            // read in the new page and reset variables associated with it
            curPageNo = nextPageNo;
            curDirtyFlag = false;
            status = bufMgr->readPage(filePtr, curPageNo, curPage);
            if (status != OK)
            {
                return status;
            }
            // checks if there are records in the page
            status = curPage->firstRecord(curRec);
        }
        // get current record
        // curRec = nextRid;
        status = curPage->getRecord(curRec, rec);
        if (status != OK)
        {
            return status;
        }
        if (matchRec(rec))
        {
            matchFound = true;
            outRid = curRec;
        }
    }

    if (matchFound)
    {
        return OK;
    }
    else
    {
        return status;
    }
}

// returns pointer to the current record.  page is left pinned
// and the scan logic is required to unpin the page

const Status HeapFileScan::getRecord(Record &rec)
{
    return curPage->getRecord(curRec, rec);
}

// delete record from file.
const Status HeapFileScan::deleteRecord()
{
    Status status;

    // delete the "current" record from the page
    status = curPage->deleteRecord(curRec);
    curDirtyFlag = true;

    // reduce count of number of records in the file
    headerPage->recCnt--;
    hdrDirtyFlag = true;
    return status;
}

// mark current page of scan dirty
const Status HeapFileScan::markDirty()
{
    curDirtyFlag = true;
    return OK;
}

const bool HeapFileScan::matchRec(const Record &rec) const
{
    // no filtering requested
    if (!filter)
        return true;

    // see if offset + length is beyond end of record
    // maybe this should be an error???
    if ((offset + length - 1) >= rec.length)
        return false;

    float diff = 0; // < 0 if attr < fltr
    switch (type)
    {

    case INTEGER:
        int iattr, ifltr; // word-alignment problem possible
        memcpy(&iattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ifltr,
               filter,
               length);
        diff = iattr - ifltr;
        break;

    case FLOAT:
        float fattr, ffltr; // word-alignment problem possible
        memcpy(&fattr,
               (char *)rec.data + offset,
               length);
        memcpy(&ffltr,
               filter,
               length);
        diff = fattr - ffltr;
        break;

    case STRING:
        diff = strncmp((char *)rec.data + offset,
                       filter,
                       length);
        break;
    }

    switch (op)
    {
    case LT:
        if (diff < 0.0)
            return true;
        break;
    case LTE:
        if (diff <= 0.0)
            return true;
        break;
    case EQ:
        if (diff == 0.0)
            return true;
        break;
    case GTE:
        if (diff >= 0.0)
            return true;
        break;
    case GT:
        if (diff > 0.0)
            return true;
        break;
    case NE:
        if (diff != 0.0)
            return true;
        break;
    }

    return false;
}

InsertFileScan::InsertFileScan(const string &name,
                               Status &status) : HeapFile(name, status)
{
    // Do nothing. Heapfile constructor will bread the header page and the first
    //  data page of the file into the buffer pool
}

InsertFileScan::~InsertFileScan()
{
    Status status;
    // unpin last page of the scan
    if (curPage != NULL)
    {
        status = bufMgr->unPinPage(filePtr, curPageNo, true);
        curPage = NULL;
        curPageNo = 0;
        if (status != OK)
            cerr << "error in unpin of data page\n";
    }
}

/**
 * Inserts the record described by rec into the file returning the RID of the
 * inserted record in outRid
 * @param rec - record to insert
 * @param outRid - RID of the inserted record
 * @return  - returns OK if no errors occured. Otherwise, return the error
 * code of the first error occurred
 */
// Insert a record into the file
const Status InsertFileScan::insertRecord(const Record &rec, RID &outRid)
{
    Page *newPage;
    int newPageNo;
    Status status, unpinstatus;
    RID rid;

    // check for very large records
    if ((unsigned int)rec.length > PAGESIZE - DPFIXED)
    {
        // will never fit on a page, so don't even bother looking
        return INVALIDRECLEN;
    }

    // adds last page to the buffer pool and reads in the last page's contents in to curPage
    if (curPage == NULL)
    {
        status = bufMgr->readPage(filePtr, headerPage->lastPage, curPage);
        if (status != OK)
        {
            return status;
        }
        curPageNo = headerPage->lastPage;
    }

    // try to insert record using Page.C's insert record
    status = curPage->insertRecord(rec, rid);
    if (status == NOSPACE)
    {
        status = bufMgr->allocPage(filePtr, newPageNo, newPage);
        if (status != OK)
        {
            return status;
        }
        newPage->init(newPageNo);
        newPage->setNextPage(-1);
        // in place insert
        int tmpRid;
        curPage->getNextPage(tmpRid);
        curPage->setNextPage(newPageNo);
        newPage->setNextPage(tmpRid);
        // if curPage was the last page, make newPage the last page
        if (tmpRid == -1)
        {
            headerPage->lastPage = newPageNo;
        }
        headerPage->pageCnt++;
        unpinstatus = bufMgr->unPinPage(filePtr, curPageNo, curDirtyFlag);
        if (unpinstatus != OK)
        {
            return status;
        }
        curPage = newPage;
        curPageNo = newPageNo;
        status = curPage->insertRecord(rec, rid);
        if (status != OK)
        {
            return status;
        }
    }
    headerPage->recCnt++;
    hdrDirtyFlag = true;
    curDirtyFlag = true;
    outRid = rid;
    return status;
}