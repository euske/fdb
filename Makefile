# Makefile

PYTHON=python
RM=rm -f
FDB=$(PYTHON) fdb.py
TESTDIR=./tmp
TESTDATA=./testdata

all:

clean:
	-$(RM) -r $(TESTDIR)

test: $(TESTDATA)
	$(FDB) $(TESTDIR) add $(TESTDATA)
	$(FDB) $(TESTDIR) add $(TESTDATA)
	$(FDB) $(TESTDIR) list
