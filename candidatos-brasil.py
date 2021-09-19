#!/usr/bin/env python
# coding: utf-8

# In[1]:


spark


# In[8]:


from spark.sql import functions as f

import pandas as pd

pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 150)


# In[4]:


latin_1 = "ISO-8859-1"

cassacao = (
    spark
    .read
    .csv("gs://gchamon-igti-deng-cloud-proc-datalake/raw/motivo_cassacao_2020_BRASIL.csv",
         sep=";",
         encoding=latin_1,
         escape="\"",
         header=True)
)


# In[5]:


cassacao.limit(1).toPandas()


# In[6]:


candidatos = (
    spark
    .read
    .csv("gs://gchamon-igti-deng-cloud-proc-datalake/raw/consulta_cand_2020_BRASIL.csv",
         sep=";",
         encoding=latin_1,
         escape="\"",
         header=True)
)


# In[9]:


candidatos.limit(1).toPandas()


# In[10]:


bens_candidatos = (candidatos
    spark
    .read
    .csv("gs://gchamon-igti-deng-cloud-proc-datalake/raw/bem_candidato_2020_BRASIL.csv",
         sep=";",
         encoding=latin_1,
         escape="\"",
         header=True)
)


# In[27]:


bens_candidatos.limit(1).toPandas()


# In[12]:


cassacao.write.mode("overwrite").parquet("gs://gchamon-igti-deng-cloud-proc-datalake/staging/cassacao")
candidatos.write.mode("overwrite").parquet("gs://gchamon-igti-deng-cloud-proc-datalake/staging/candidatos")
bens_candidatos.write.mode("overwrite").parquet("gs://gchamon-igti-deng-cloud-proc-datalake/staging/bens_candidatos")


# In[14]:


candidatos = (
    spark
    .read
    .parquet("gs://gchamon-igti-deng-cloud-proc-datalake/staging/candidatos")
)
bens_candidatos = (
    spark
    .read
    .parquet("gs://gchamon-igti-deng-cloud-proc-datalake/staging/bens_candidatos")
)
cassacao = (
    spark
    .read
    .parquet("gs://gchamon-igti-deng-cloud-proc-datalake/staging/cassacao")
)


# In[38]:


candidatos_bens_columns = set(candidatos.columns).intersection(bens_candidatos.columns)

candidatos_bens = (
    candidatos
    .join(bens_candidatos, list(candidatos_bens_columns), "left")
)

candidatos_bens_cassacao_columns = set(candidatos_bens.columns).intersection(cassacao.columns)

candidatos_bens_cassacao = (
    candidatos_bens
    .join(cassacao, list(candidatos_bens_cassacao_columns), "left")
)

candidatos_bens_cassacao.limit(5).toPandas()


# In[32]:


candidatos_bens_cassacao.limit(5).toPandas()


# In[39]:


(
    candidatos_bens_cassacao
    .write
    .mode("overwrite")
    .partitionBy("SG_UF")
    .parquet("gs://gchamon-igti-deng-cloud-proc-datalake/curated/candidatos_bens_cassacao")
)


# In[40]:


(
    candidatos_bens_cassacao
    .write
    .mode("overwrite")
    .partitionBy("SG_UF")
    .csv("gs://gchamon-igti-deng-cloud-proc-datalake/human_readable/candidatos_bens_cassacao")
)

