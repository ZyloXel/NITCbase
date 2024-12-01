#include "Algebra.h"

#include <cstring>
#include <stdlib.h>
#include <stdio.h>

// Function to check if a string represents a number
bool isNumber(char *str)
{
    int len;
    float ignore;

    // sscanf checks if the string can be parsed as a float
    // %f reads a floating-point number, %n records the number of characters processed
    int ret = sscanf(str, " %f %n", &ignore, &len);

    // The string is a valid number if sscanf returns 1 (successful conversion) and
    // len matches the string length (ensures the whole string was a number)
    return ret == 1 && len == strlen(str);
}

/* Function to select all records from a source relation (table) that satisfy a given condition.
 * Parameters:
 * - srcRel: The name of the source relation from which to select records.
 * - targetRel: The name of the target relation to select into (currently ignored).
 * - attr: The name of the attribute to apply the condition on.
 * - op: The operator used for the condition (e.g., EQ for equality).
 * - strVal: The value to compare against, represented as a string.
 */
int Algebra::select(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE], char attr[ATTR_SIZE], int op, char strVal[ATTR_SIZE]){
    int srcRelId = OpenRelTable::getRelId(srcRel);

    
    if(srcRelId == E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }

    AttrCatEntry attrCatEntry;

    int ret = AttrCacheTable::getAttrCatEntry(srcRelId,attr,&attrCatEntry);

    // std::cout<<"ret: "<<ret<<std::endl;
    if(ret != SUCCESS){
        return ret;
    }

    /*** Convert strVal (string) to an attribute of data type NUMBER or STRING ***/

    int type = attrCatEntry.attrType;

    Attribute attrVal;

    if(type == NUMBER){   // the isNumber() function is implemented below
        if(isNumber(strVal)){
            attrVal.nVal = atof(strVal);
        }else{
            return E_ATTRTYPEMISMATCH;
        }
    }else if(type == STRING){
        strcpy(attrVal.sVal,strVal);
    }

    /*** Selecting records from the source relation ***/

    // Before calling the search function, reset the search to start from the first hit
    // using RelCacheTable::resetSearchIndex()

    RelCatEntry relCatEntry;

    // get relCatEntry using RelCacheTable::getRelCatEntry()

    RelCacheTable::getRelCatEntry(srcRelId,&relCatEntry);

     /*** Creating and opening the target relation ***/
    // Prepare arguments for createRel() in the following way:
    // get RelcatEntry of srcRel using RelCacheTable::getRelCatEntry()
    int src_nAttrs = relCatEntry.numAttrs/* the no. of attributes present in src relation */ ;

    char attr_names[src_nAttrs][ATTR_SIZE];

    int attr_types[src_nAttrs];

    /*iterate through 0 to src_nAttrs-1 :
        get the i'th attribute's AttrCatEntry using AttrCacheTable::getAttrCatEntry()
        fill the attr_names, attr_types arrays that we declared with the entries
        of corresponding attributes
    */

    for(int i=0;i<src_nAttrs;i++){

        AttrCatEntry attrCatEntry;
        AttrCacheTable::getAttrCatEntry(srcRelId,i,&attrCatEntry);
        strcpy(attr_names[i],attrCatEntry.attrName);
        attr_types[i] = attrCatEntry.attrType;

    }

    /* Create the relation for target relation by calling Schema::createRel()
       by providing appropriate arguments */
    // if the createRel returns an error code, then return that value.

    ret = Schema::createRel(targetRel,src_nAttrs,attr_names,attr_types);

    if(ret != SUCCESS)return ret;


    /* Open the newly created target relation by calling OpenRelTable::openRel()
       method and store the target relid */
    /* If opening fails, delete the target relation by calling Schema::deleteRel()
       and return the error value returned from openRel() */

    int targetRelId = OpenRelTable::openRel(targetRel);

    if(targetRelId < 0 || targetRelId > MAX_OPEN ){
        Schema::deleteRel(targetRel);
        return targetRelId;
    }

    /*** Selecting and inserting records into the target relation ***/
    /* Before calling the search function, reset the search to start from the
       first using RelCacheTable::resetSearchIndex() */

    

    Attribute record[src_nAttrs];

    /*
        The BlockAccess::search() function can either do a linearSearch or
        a B+ tree search. Hence, reset the search index of the relation in the
        relation cache using RelCacheTable::resetSearchIndex().
        Also, reset the search index in the attribute cache for the select
        condition attribute with name given by the argument `attr`. Use
        AttrCacheTable::resetSearchIndex().
        Both these calls are necessary to ensure that search begins from the
        first record.
    */

    RelCacheTable::resetSearchIndex(srcRelId);
    // AttrCacheTable::resetSearchIndex(/* fill arguments */);

    // read every record that satisfies the condition by repeatedly calling
    // BlockAccess::search() until there are no more records to be read


    while(true){

        Attribute record[src_nAttrs];

        int ret = BlockAccess::search(srcRelId,record,attr,attrVal,op);

        if(ret != SUCCESS)break;

        ret = BlockAccess::insert(targetRelId, record);


        // if (insert fails) {
        //     close the targetrel(by calling Schema::closeRel(targetrel))
        //     delete targetrel (by calling Schema::deleteRel(targetrel))
        //     return ret;
        // }

        if(ret != SUCCESS){
            Schema::closeRel(targetRel);
            Schema::deleteRel(targetRel);
            return ret;
        }
        
    }

    Schema::closeRel(targetRel);

    return SUCCESS;
   
}

// Stage 7
int Algebra::insert(char relName[ATTR_SIZE], int nAttrs, char record[][ATTR_SIZE])
{
    // if relName is equal to "RELATIONCAT" or "ATTRIBUTECAT"
    // return E_NOTPERMITTED;
    if (strcmp(relName, (char *)RELCAT_RELNAME) == 0 || strcmp(relName, (char *)ATTRCAT_RELNAME) == 0)
    {
        return E_NOTPERMITTED;
    }

    // get the relation's rel-id using OpenRelTable::getRelId() method
    int relId = OpenRelTable::getRelId(relName);

    // if relation is not open in open relation table, return E_RELNOTOPEN
    // (check if the value returned from getRelId function call = E_RELNOTOPEN)
    if (relId == E_RELNOTOPEN)
    {
        return E_RELNOTOPEN;
    }
    // get the relation catalog entry from relation cache
    // (use RelCacheTable::getRelCatEntry() of Cache Layer)
    RelCatEntry relCatBuf;
    RelCacheTable::getRelCatEntry(relId, &relCatBuf);

    /* if relCatEntry.numAttrs != numberOfAttributes in relation,
       return E_NATTRMISMATCH */
    if (relCatBuf.numAttrs != nAttrs)
    {
        return E_NATTRMISMATCH;
    }

    // let recordValues[numberOfAttributes] be an array of type union Attribute
    Attribute recordValues[nAttrs];

        /*
            Converting 2D char array of record values to Attribute array recordValues
         */
        // iterate through 0 to nAttrs-1: (let i be the iterator)
        for(int i = 0; i < nAttrs; i++){
        // get the attr-cat entry for the i'th attribute from the attr-cache
        // (use AttrCacheTable::getAttrCatEntry())
            AttrCatEntry attrCatBuf;
            AttrCacheTable::getAttrCatEntry(relId,i,&attrCatBuf);

        // let type = attrCatEntry.attrType;

        if (attrCatBuf.attrType == NUMBER)
        {
            // if the char array record[i] can be converted to a number
            // (check this using isNumber() function)
            if(isNumber(record[i]))
            {
                /* convert the char array to numeral and store it
                   at recordValues[i].nVal using atof() */
                recordValues[i].nVal=atof(record[i]);
            }
            else
            {
                return E_ATTRTYPEMISMATCH;
            }
        }
        else if (attrCatBuf.attrType == STRING)
        {
            // copy record[i] to recordValues[i].sVal
            strcpy(recordValues[i].sVal,record[i]);
        }

        }

    // insert the record by calling BlockAccess::insert() function
    // let retVal denote the return value of insert call
    int retVal=BlockAccess::insert(relId,recordValues);
    return retVal;
}

//Stage 9
//Select already implemented
int Algebra::project(char srcRel[ATTR_SIZE], char targetRel[ATTR_SIZE]) {

    int srcRelId = OpenRelTable::getRelId(srcRel);/*srcRel's rel-id (use OpenRelTable::getRelId() function)*/

    // if srcRel is not open in open relation table, return E_RELNOTOPEN
    if(srcRelId==E_RELNOTOPEN){
        return E_RELNOTOPEN;
    }

    // get RelCatEntry of srcRel using RelCacheTable::getRelCatEntry()
    RelCatEntry relCatEntry;

    RelCacheTable::getRelCatEntry(srcRelId,&relCatEntry);

    // get the no. of attributes present in relation from the fetched RelCatEntry.
    int numAttrs=relCatEntry.numAttrs;

    // attrNames and attrTypes will be used to store the attribute names
    // and types of the source relation respectively
    char attrNames[numAttrs][ATTR_SIZE];
    int attrTypes[numAttrs];

    /*iterate through every attribute of the source relation :
        - get the AttributeCat entry of the attribute with offset.
          (using AttrCacheTable::getAttrCatEntry())
        - fill the arrays `attrNames` and `attrTypes` that we declared earlier
          with the data about each attribute
    */
   for(int i=0;i<numAttrs;i++){
    AttrCatEntry attrCatEntry;
    AttrCacheTable::getAttrCatEntry(srcRelId,i,&attrCatEntry);
    strcpy(attrNames[i],attrCatEntry.attrName);
    attrTypes[i] = attrCatEntry.attrType;
   }


    /*** Creating and opening the target relation ***/

    // Create a relation for target relation by calling Schema::createRel()
    int ret=Schema::createRel(targetRel,numAttrs,attrNames,attrTypes);

    // if the createRel returns an error code, then return that value.
    if(ret!=SUCCESS){
        return ret;
    }

    // Open the newly created target relation by calling OpenRelTable::openRel()
    // and get the target relid
    int targetRelId=

    // If opening fails, delete the target relation by calling Schema::deleteRel() of
    // return the error value returned from openRel().


    /*** Inserting projected records into the target relation ***/

    // Take care to reset the searchIndex before calling the project function
    // using RelCacheTable::resetSearchIndex()

    Attribute record[numAttrs];


    while (/* BlockAccess::project(srcRelId, record) returns SUCCESS */)
    {
        // record will contain the next record

        // ret = BlockAccess::insert(targetRelId, proj_record);

        if (/* insert fails */) {
            // close the targetrel by calling Schema::closeRel()
            // delete targetrel by calling Schema::deleteRel()
            // return ret;
        }
    }

    // Close the targetRel by calling Schema::closeRel()

    // return SUCCESS.
}