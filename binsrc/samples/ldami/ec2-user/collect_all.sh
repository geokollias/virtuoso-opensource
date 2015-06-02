rm -rf /home/collection

mkdir /home/collection
cd /home/collection
mkdir ec2-user snb30 snb300 snbval  spb1g  spb256  spbval  tpch100  tpch1000c  tpch300

cd /home/ec2-user
cp collect_all.sh hosts.conf mk.sh mkvol.sh np.cmd README setup_all.sh setup_one.sh /home/collection/ec2-user

cd /home/snb30
cp -r chech.sql dbget.sh report.sh rm_database.sh run_validator.awk warmup.sh dbgen.sh dbput_db.sh ldbc_driver_default.properties load.sh params.ini rm_dataset.sh virtuoso_configuration.properties dbget_db.sh dbput.sh ldbc_snb_interactive_SF-0030.properties numbers.sh README results.sql run.sh virtuoso.ini /home/collection/snb30

cd /home/snb300
cp -r chech.sql dbget.sh load.out numbers.txt report.sh run.sh virtuoso.ini dbgen.sh dbput_db.sh ldbc_driver_default.properties load.sh rm_database.sh run_validator.awk dbget_db.sh dbput.sh ldbc_snb_interactive_SF-0300.properties numbers.sh params.ini rm_dataset.sh virtuoso_configuration.properties /home/collection/snb300

cd /home/snbval
cp ldbc_driver_default.properties  ldbc_snb_interactive.properties  validate.sh virtuoso_configuration.properties  virtuoso.ini /home/collection/snbval

cd /home/spb1g
cp dbcpy.sh load.sh README rm_database.sh rm_dataset.sh run.sh spb1ggen.properties spb1g.properties spb1gqgen.properties virtuoso.ini  /home/collection/spb1g

cd /home/spb256
cp load.sh README rm_database.sh rm_dataset.sh run.sh spb256gen.properties spb256.properties spb256qgen.properties virtuoso.ini  /home/collection/spb256

cd /home/spbval
cp load.sh rm_database.sh run.sh validation.properties virtuoso.ini  /home/collection/spbval

cd /home/tpch100
cp datagen.sh load.sh rm_database.sh rm_dataset.sh run.sh virtuoso.ini  /home/collection/tpch100

cd /home/tpch1000c
cp back_to_orig_state.sh clrefresh.sql.tpl cluster.ini.tpl dists.dss init.sql ldschema.sql rm_dataset.sh schema.sql  virtuoso.global.ini.tpl virtuoso.ini.tpl clld.sql cluster.global.ini.tpl init_all.sh ldfile.sql load.sh run.sh tpc-h.sql virtuoso.ini /home/collection/tpch1000c

cd /home/tpch300
cp clld.sql clrefresh.sql datagen.sh dbgen dbgen.sh dists.dss load.sh rm_database.sh rm_dataset.sh run.sh virtuoso.ini  /home/collection/tpch300
