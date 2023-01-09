#include <stdlib.h>
#include "catalog.h"
#include "query.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string &relation,
                       const string &attrName,
                       const Operator op,
                       const Datatype type,
                       const char *attrValue)

{

  // check if relation table is empty then proceed
  if (relation.empty())
  {
    return BADCATPARM;
  }

  Status status;
  RID rid;
  AttrDesc attrD;
  HeapFileScan *scanner;

  // Where clause is empty
  if (attrName.empty())
  {

    // creating scanner object
    scanner = new HeapFileScan(relation, status);
    if (status != OK)
    {
      return status;
    }

    // start the scan
    scanner->startScan(0, 0, STRING, NULL, EQ);
    // iterate and call next in scanner object to go over the results and deleting the records
    while ((status = scanner->scanNext(rid)) != FILEEOF)
    {
      status = scanner->deleteRecord();
      if (status != OK)
      {
        return status;
      }
    }
  }

  else
  {

    // creating Scanner object
    scanner = new HeapFileScan(relation, status);
    if (status != OK)
    {
      return status;
    }

    // getting information about attribute
    status = attrCat->getInfo(relation, attrName, attrD);
    if (status != OK)
    {
      return status;
    }

    // Switch which starts scan based on data type
    int intVal;
    float floatVal;
    switch (type)
    {
    case INTEGER:

      intVal = atoi(attrValue);
      status = scanner->startScan(attrD.attrOffset, attrD.attrLen, type, (char *)&intVal, op);
      break;

    case FLOAT:
      floatVal = atof(attrValue);
      status = scanner->startScan(attrD.attrOffset, attrD.attrLen, type, (char *)&floatVal, op);
      break;

      // default condition i.e string
    default:
      status = scanner->startScan(attrD.attrOffset, attrD.attrLen, type, attrValue, op);
      break;
    }

    if (status != OK)
    {
      return status;
    }

    // iterate and call next in scanner object to go over the results and deleting the records
    while ((status = scanner->scanNext(rid)) == OK)
    {
      status = scanner->deleteRecord();
      if (status != OK)
      {
        return status;
      }
    }
  }

  if (status != FILEEOF)
  {
    return status;
  }

  // scan finished
  scanner->endScan();
  delete scanner;

  return OK;
}
