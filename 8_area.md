# Robustness and fault tolerance:
 - My data is going to be written in parquet on a daily basis. So if
   the pipeline breaks I still have the data on the day before

# Low latency reads and updates:
 - The process that takes the longest for me is to write the raw data
   into parquets.

# Scalability:
 - Not really, if I were to scale out this project, I will need to do
   spark streaming.

# Generalization:
 - The code is specific to this project, but the idea is very general.

# Extensibility:
  - Yes

# Ad hoc queries
 - parquet is easy for ad hoc queries

# Minimal maintenance
 - Yes

# Debuggability
 - hard, emr with steps is always giving me errors.
