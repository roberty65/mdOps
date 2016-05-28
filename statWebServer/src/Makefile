INC = -I ../../../bServer/common/include \
      -I ../../../bServer/frame/share/include \
      -I ../../statShare/include
LIB = -L ../../statShare/lib -lstatShare \
      -L ../../../bServer/frame/share/lib -lProcessor \
      -L ../../../bServer/common/lib -lcommon \
      -lpthread


CXXFLAGS = -Wall -g -fPIC #-O3
LDFLAGS  =

DEST = ../cgi/systemMetrics.cgi
OBJS = QueryParameters.o ClientConnection.o SystemMetrics.o

.PHONY: mkdirs all clean distclean

all: mkdirs $(DEST)

$(DEST): $(OBJS)
	g++ -o $@ $(LDFLAGS) $(OBJS) $(LIB)
.cpp.o:
	g++ -c -o $@ $(INC) $(CXXFLAGS) $<
.c.o:
	gcc -c -o $@ $(INC) $(CXXFLAGS) $<
mkdirs:
	mkdir -p ../cgi ../bin
clean:
	rm -f $(OBJS) *~ *.s *.ii *.i
distclean: clean
	rm -f $(DEST)
