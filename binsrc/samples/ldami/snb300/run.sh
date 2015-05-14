#!/bin/bash

java -Xmx8g -cp /home/ldbc_driver/target/jeeves-0.2-SNAPSHOT.jar:/home/ldbc_snb_implementations/interactive/virtuoso/java/virtuoso/target/virtuoso-0.0.1-SNAPSHOT.jar:/home/ldbc_snb_implementations/interactive/virtuoso/java/virtuoso/virtjdbc4_lib/virtjdbc4.jar com.ldbc.driver.Client -db com.ldbc.driver.workloads.ldbc.snb.interactive.db.VirtuosoDb -P ./ldbc_snb_interactive_SF-0300.properties -P ./ldbc_driver_default.properties -P virtuoso_configuration.properties -P /home/snb300/outputDir/updates/updateStream.properties -oc 4000000
