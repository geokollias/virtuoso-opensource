SCALE?=		1
HEAD=		head
DBGEN_DIR=	../../tpc-h/dbgen
QUERY_FILES=	1.sql 2.sql 3.sql 4.sql 5.sql 6.sql 7.sql 8.sql 9.sql 10.sql 11.sql\
		12.sql 13.sql 14.sql 15.sql 16.sql 17.sql 18.sql 19.sql 20.sql 21.sql 22.sql
DATA_FILES=	customer.tbl lineitem.tbl nation.tbl orders.tbl part.tbl partsupp.tbl region.tbl supplier.tbl

all: queries data

queries: $(QUERY_FILES)

data: $(DATA_FILES)

$(DATA_FILES): $(DBGEN_DIR)/dbgen
	DSS_DIST=$(DBGEN_DIR)/dists.dss $(DBGEN_DIR)/dbgen -fq -s $(SCALE)

$(QUERY_FILES):	$(DBGEN_DIR)/qgen
	(DSS_DIST=$(DBGEN_DIR)/dists.dss DSS_QUERY=$(DBGEN_DIR) $(DBGEN_DIR)/qgen -d $(basename $@) | head -n -1 || rm $@) >$@

$(DBGEN_DIR)/dbgen:
	$(MAKE) -C $(DBGEN_DIR) dbgen

$(DBGEN_DIR)/qgen:
	$(MAKE) -C $(DBGEN_DIR) qgen

clean:
	rm -f $(DATA_FILES) $(QUERY_FILES)
