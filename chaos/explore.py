from pyspark.sql import SparkSession
s=SparkSession.builder.appName("explore").getOrCreate()
s.sparkContext.setLogLevel("INFO")
s.sql("CREATE NAMESPACE IF NOT EXISTS ice.db")
s.sql("DROP TABLE IF EXISTS ice.db.ex PURGE")
s.sql("CREATE TABLE ice.db.ex (batch_id int,rid long,payload string) USING iceberg")
for b in range(8):
    s.range(b*1000,b*1000+1000).selectExpr(f"{b} as batch_id","id as rid","repeat('x',50) as payload").repartition(4).writeTo("ice.db.ex").append()
print("FILES",s.sql("select count(*) from ice.db.ex.files").collect())
r=s.sql("CALL ice.system.rewrite_data_files(table=>'db.ex', options=>map('partial-progress.enabled','true','partial-progress.max-commits','4','max-file-group-size-bytes','60000','min-input-files','2'))").collect()
print("RESULT",r)
print(s.sql("select snapshot_id,operation,summary['added-data-files'] a,summary['deleted-data-files'] d from ice.db.ex.snapshots").collect()[-6:])
