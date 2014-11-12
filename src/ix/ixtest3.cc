#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "ix.h"
#include "ixtest_util.h"

IndexManager *indexManager;

int testCase_3(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Destroy Index File **
    // 2. Open Index File -- should fail
    // 3. Scan  -- should fail
    cout << endl << "****In Test Case 3****" << endl;

    RC rc;
    IXFileHandle ixfileHandle;
    IX_ScanIterator ix_ScanIterator;

    // destroy index file
    rc = indexManager->destroyFile(indexFileName);
    if(rc != success)
    {
        cout << "Failed Destroying Index File..." << endl;
        return fail;
    }

    // open the destroyed index
    rc = indexManager->openFile(indexFileName, ixfileHandle);
    if(rc == success) //should not work now
    {
        cout << "Index opened again...failure" << endl;
        indexManager->closeFile(ixfileHandle);
        return fail;
    }

    // open scan
    rc = indexManager->scan(ixfileHandle, attribute, NULL, NULL, true, true, ix_ScanIterator);
    if(rc == success)
    {
        cout << "Scan opened again...failure" << endl;
        ix_ScanIterator.close();
        return fail;
    }

    return success;

}

int main()
{
    //Global Initializations
    indexManager = IndexManager::instance();

	const string indexFileName = "age_idx";
	Attribute attrAge;
	attrAge.length = 4;
	attrAge.name = "age";
	attrAge.type = TypeInt;

    RC result = testCase_3(indexFileName, attrAge);;
    if (result == success) {
    	cout << "IX_Test Case 3 passed" << endl;
    	return success;
    } else {
    	cout << "IX_Test Case 3 failed" << endl;
    	return fail;
    }

}

