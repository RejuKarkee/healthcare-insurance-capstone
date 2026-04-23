from pyspark.sql import SparkSession, functions as F

# Creates the Spark engine
spark = SparkSession.builder.appName("healthcare-capstone").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Connects to AWS S3 using your credentials
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "AKIAXTSRYF4L7FL3COBG")
hadoop_conf.set("fs.s3a.secret.key", "ALMHRdmreeN3dkz709An4q1bH20bqvNEcpvfro75")
hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")


# Defines where your data lives
BUCKET = "bucket-takeoai"
INPUT = f"s3a://{BUCKET}/input-data"
OUTPUT = f"s3a://{BUCKET}/output-data/"

print("STARTING CAPSTONE JOB")

# Reading raw data from S3
subscriber_df   = spark.read.option("header", "true").csv(f"{INPUT}/subscriber.csv")
disease_df      = spark.read.option("header", "true").csv(f"{INPUT}/disease.csv")
group_df   = spark.read.option("header", "true").csv(f"{INPUT}/group.csv")
grpsubgrp_df      = spark.read.option("header", "true").csv(f"{INPUT}/grpsubgrp.csv")
hospital_df   = spark.read.option("header", "true").csv(f"{INPUT}/hospital.csv")
patient_records_df      = spark.read.option("header", "true").csv(f"{INPUT}/Patient_records.csv")
subgroup_df   = spark.read.option("header", "true").csv(f"{INPUT}/subgroup.csv")
claims_df      = spark.read.option("multiline", "true").json(f"{INPUT}/claims.json")

print("Subscriber count:", subscriber_df.count())
print("Disease count:", disease_df.count())
print("Hospital count:", hospital_df.count())
print("Patient count:", patient_records_df.count())
print("Claims count:", claims_df.count())
print("Group count:", group_df.count())
print("Subgroup count:", subgroup_df.count())
print("Group-Subgroup count:", grpsubgrp_df.count())

# DATA CLEANING
print("\n[INFO] Starting Data Cleaning")

#Drop unnecessary columns
# Disease table has an extra SubGrpID column that doesn't belong
disease_df = disease_df.drop("SubGrpID")


#Standardize all column names
#Rename all columns to lowercase, Strip leading/trailing spaces and replace spaces with underscores
def standardize_columns(df):
    for col in df.columns:
        new_name = col.lower().strip().replace(" ", "_")
        df = df.withColumnRenamed(col, new_name)
    return df

disease_df = standardize_columns(disease_df)
subgroup_df = standardize_columns(subgroup_df)
subscriber_df = standardize_columns(subscriber_df)
claims_df = standardize_columns(claims_df)
hospital_df = standardize_columns(hospital_df)
group_df = standardize_columns(group_df)
grpsubgrp_df  = standardize_columns(grpsubgrp_df)
patient_records_df = standardize_columns(patient_records_df)

print("\n[INFO] Column name standardized for all tables")

# Subscriber sub__id has double underscore due to leading space in raw data
subscriber_df = subscriber_df.withColumnRenamed("sub__id", "sub_id")

#replace NaN string with  proper NULL values across all tables
def clean_nan(df):
    string_cols = [c for c, t in df.dtypes if t == "string"]
    for c in string_cols:
        df = df.withColumn(c,
                           F.when(F.col(c) == "NaN", None).otherwise(F.col(c)))
    return df

claims_df = clean_nan(claims_df)
subscriber_df = clean_nan(subscriber_df)
hospital_df = clean_nan(hospital_df)
group_df = clean_nan(group_df)
subgroup_df = clean_nan(subgroup_df)
grpsubgrp_df = clean_nan(grpsubgrp_df)
patient_records_df = clean_nan(patient_records_df)
disease_df = clean_nan(disease_df)

print("\n[INFO] NaN values replaces with NULL across all tables.")

#FIX DATA TYPES ISSUES
# Subscriber: Convert elig_ind from Y/N to Boolean
subscriber_df = subscriber_df.withColumn("elig_ind",
                                         F.when(F.col("elig_ind") == "Y", True).otherwise(False))

# Hospital: Fill missing state values with "Unknown"
hospital_df = hospital_df.fillna("Unknown", subset=["state"])

print("[INFO] Data types fixed")

#REPLACE disease_name with disease_id in claims and patient tables

#Claims table
claims_df = claims_df.join(
    disease_df.select("disease_id", "disease_name"),
    on = "disease_name",
    how="left"
)

claims_df = claims_df.drop("disease_name")

#Patient table
patient_records_df = patient_records_df.join(
    disease_df.select("disease_id", "disease_name"),
    on="disease_name",
    how="left"
)

patient_records_df = patient_records_df.drop("disease_name")

print("[INFO] disease_name replaced with disease_id in Claims and Patient tables")

#CHECK NULL VALUES FOR REQUIRED TABLE
def check_nulls(df, table_name):
    print(f"\n=== NULL COUNT - {table_name.upper()} ===")
    df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c)
        for c in df.columns
    ]).show()
    print(f"Total rows   : {df.count()}")
    print(f"Distinct rows: {df.distinct().count()}")


check_nulls(subscriber_df, "Subscriber")
check_nulls(claims_df, "Claims")
check_nulls(patient_records_df, "Patient")
check_nulls(group_df, "Group")
check_nulls(subgroup_df, "Subgroup")

#FIX NULL VALUES FOR REQUIRED TABLES
#Subscriber
subscriber_df = subscriber_df.fillna("NA", subset=["first_name", "phone", "subgrp_id"])

# Claims
claims_df = claims_df.fillna("NA", subset=["claim_or_rejected", "claim_type"])

# Patient
patient_records_df = patient_records_df.fillna("NA", subset=["patient_name", "patient_phone"])

# Group
group_df = group_df.fillna("NA", subset=["grp_name", "grp_type", "city"])

# Subgroup
subgroup_df = subgroup_df.fillna("NA", subset=["subgrp_name"])

#DROP duplicates across all tables
subscriber_df = subscriber_df.dropDuplicates()
disease_df = disease_df.dropDuplicates()
group_df = group_df.dropDuplicates()
grpsubgrp_df = grpsubgrp_df.dropDuplicates()
hospital_df = hospital_df.dropDuplicates()
patient_records_df = patient_records_df.dropDuplicates()
subgroup_df = subgroup_df.dropDuplicates()
claims_df = claims_df.dropDuplicates()
claims_df = claims_df.dropna(subset=["claim_id"])

print("\n[INFO] Duplicates dropped from all tables")

#VERIFY null counts after cleaning
print("\n[INFO] Verifying null counts after cleaning...")
check_nulls(subscriber_df, "Subscriber - AFTER CLEANING")
check_nulls(claims_df, "Claims - AFTER CLEANING")
check_nulls(patient_records_df, "Patient - AFTER CLEANING")
check_nulls(group_df, "Group - AFTER CLEANING")
check_nulls(subgroup_df, "Subgroup - AFTER CLEANING")

print("\n[INFO] Data Cleaning Complete!")

#SAVE CLEANED DATA TO S3 AS PARQUET
print("\n[INFO] Saving cleaned data to S3...")

subscriber_df.write.mode("overwrite").parquet(f"{OUTPUT}/subscriber/")
disease_df.write.mode("overwrite").parquet(f"{OUTPUT}/disease/")
group_df.write.mode("overwrite").parquet(f"{OUTPUT}/group/")
grpsubgrp_df.write.mode("overwrite").parquet(f"{OUTPUT}/grpsubgrp/")
hospital_df.write.mode("overwrite").parquet(f"{OUTPUT}/hospital/")
patient_records_df.write.mode("overwrite").parquet(f"{OUTPUT}/patient/")
subgroup_df.write.mode("overwrite").parquet(f"{OUTPUT}/subgroup/")
claims_df.write.mode("overwrite").parquet(f"{OUTPUT}/claims/")

print("[INFO] All cleaned data saved to S3 successfully!")
