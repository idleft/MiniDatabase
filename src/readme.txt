
- Modify the "CODEROOT" variable in makefile.inc to point to the root of your code base

- Integrate your own implementation of rbf to folder, "rbf".
  Note that the structure of FileHandle class has changed. You need to implement additional code.
  Refer to the project 3 page for detail.
  
- Implement the Index Manager (IX):

   Go to folder "ix" and type in:

    make clean
    make
    ./ixtest1

   The program should work.  But it will fail. 
   You are supposed to implement the API of the index manager defined in ix.h and pfm.h

   There are 14 public test cases. Refer to the each test case for detail. 

- By default you should not change those functions of the IndexManager and IX_ScanIterator class defined in ix/ix.h. 
  If you think some changes are really necessary, please contact us first.

