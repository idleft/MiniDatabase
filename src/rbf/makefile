
include ../makefile.inc

all: librbf.a rbftest rbftest16

# lib file dependencies
librbf.a: librbf.a(pfm.o)  # and possibly other .o files
librbf.a: librbf.a(rbfm.o)

# c file dependencies
pfm.o: pfm.h
rbfm.o: rbfm.h

rbftest.o: pfm.h rbfm.h

rbftest16.o: pfm.h rbfm.h

# binary dependencies
rbftest: rbftest.o librbf.a $(CODEROOT)/rbf/librbf.a

rbftest16: rbftest16.o librbf.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
clean:
	-rm rbftest rbftest16 *.a *.o *~
