

[Cluster]
ThisHost = Host{c ('proc')}

[ELASTIC]
Slices = 16
Segment1 = 1536, { dbfile (sprintf ('/1s%d/dbs/tpch1000c/tpch1000c.db', 1 + mod(c ('proc') - 1, 2))) } = q1

