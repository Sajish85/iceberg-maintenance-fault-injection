from pyspark.sql import SparkSession
s=SparkSession.builder.appName("smoke").getOrCreate()
s.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")
s.sql("DROP TABLE IF EXISTS ice.db.smoke")
s.sql("CREATE TABLE ice.db.smoke (batch_id int,row_id long,payload string) USING iceberg")
for b in range(3):
    s.range(b*10,b*10+10).selectExpr(f"{b} as batch_id","id as row_id","'x' as payload").writeTo("ice.db.smoke").append()
s.sql("select snapshot_id,operation,parent_id from ice.db.smoke.snapshots").show(truncate=False)
print(s.sql("CALL ice.system.rewrite_data_files(table=>'db.smoke')").collect())
print(s.sql("CALL ice.system.expire_snapshots(table=>'db.smoke', older_than=>TIMESTAMP '2999-01-01 00:00:00', retain_last=>1)").collect())
print(s.sql("CALL ice.system.remove_orphan_files(table=>'db.smoke', older_than=>TIMESTAMP '2999-01-01 00:00:00')").collect())
print(s.sql("select count(*) from ice.db.smoke").collect())
