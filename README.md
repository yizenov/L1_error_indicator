# Identifying Sub-optimal Query Plans via L1-error

## Table of Contents
1. [Experimental Setup](#setup)
2. [L1-error Evaluation](#evaluation)
3. [IMDB Dataset](#dataset)
4. [JOB-light and Join Order Benchmark (JOB) workloads](#benchmark)
5. [Install PostgreSQL and COMPASS](#installation)
6. [Questions](#questions)
7. [References](#references)
8. [Citation](#citation)

## 1. Experimental Setup <a name="setup"></a>
L1-error was developed in the following environment:
- Machine: 2x Intel(R) Xeon(R) CPU E5-2660 v4 (56 CPU cores and 256GB memory)
- NVIDIA Tesla K80 GPU, CUDA v11.4
- OS: Ubuntu 22.04 LTS, Clang/LLVM v14.0.0, gcc v11.3.0
- Python 3.10.6 (sklearn v1.1.2, numpy v1.21.5, pandas v1.4.3, matplotlib v3.5.3)
- Docker v20.10.12, NVIDIA Docker v2.12.0

## 2. L1-error Evaluation <a name="evaluation"></a>
We provide [Python scripts](scripts), which one can use to replicate [L1-error results](output_data). The results can be used to reproduce the [figures and tables](figures) in the paper. All the figures are stored in ods files and provided [here](figures). The [input data](input_data) for the [Python scripts](scripts) are already provided [here](input_data) with [instructions](L1_Error_Input_README.md). In case one decides to reproduce the input data, [corresponding Python scripts](scripts_prepare) are also provided.

Step-by-step instructions are provided [here](L1_Error_README.md).

## 3. IMDB Dataset <a name="dataset"></a>
The dataset that was used is [Internet Movie Data Base (IMDB)](https://www.imdb.com/). The original data is publicly available (ftp://ftp.fu-berlin.de/pub/misc/movies/database/) in txt files, and the open-source [imdbpy package](https://bitbucket.org/alberanid/imdbpy/get/5.0.zip) was used to transform txt files to CSV files in [[1]](#1). See more details [here](https://github.com/gregrahn/join-order-benchmark). This 3.6GB snapshot is from May 2013, and it can be downloaded from [here](homepages.cwi.nl/~boncz/job/imdb.tgz). The dataset includes 21 CSV files i.e., 21 relations in total. The package also includes queries to create the necessary relations written in `schema.sql` or `schematext.sql` files. Lastly, in addition to primary keys, there are queries to create foreign keys in case one decides to use them.

However, there were issues in bulk loading the original dataset, SQL syntax errors, and missing primary key values in referenced tables. To make sure PostgreSQL and COMPASS have the same dataset, we fixed those errors in data, schema, and indexes. We shared the updated version [here](https://ucmerced.app.box.com/folder/164372766742?v=jobimdbdataworkload). Both docker images are already loaded with the updated version.

## 4. JOB-light and Join Order Benchmark (JOB) <a name="benchmark"></a>
The workloads used to evaluate L1-error are [JOB-light](https://github.com/andreaskipf/learnedcardinalities) and [Join Order Benchmark (JOB)](http://www-db.in.tum.de/~leis/qo/job.tgz).
- JOB-light consists of 70 star-structure queries with equijoins. Join sizes 2-5, join predicates 1-4, and tables 2-5. The first three queries consist of only two tables.
- JOB consists of 113 queries in total, including 33 query families with equijoins, and each family's queries differ only in selection predicates. Join sizes 2-17, join predicates 4-28, and tables 2-17.

There were differences in SQL syntax and their execution. Thus the results of selection predicates in PostgreSQL and COMPASS were different. Thus we adjusted the queries so that both systems have the same selectivities and cardinality. We shared the updated version [here](https://ucmerced.app.box.com/folder/164372766742?v=jobimdbdataworkload). Both docker images and current work are already loaded with the updated version.

## 5. Install PostgreSQL and COMPASS <a name="installation"></a>
We collected cardinality estimations from one well-known and one recently proposed query optimizer:
- [PostgreSQL](https://www.postgresql.org) v15.1
- COMPASS is built as an extension on top of a clone of [MapD System](https://github.com/heavyai/heavydb), version 3.6.1, rebranded to OmniSciDB and then to [HeavyDB](https://www.heavy.ai)

We prepared corresponding Docker images with the same configurations as in the paper. One can simply pull the images from [Docker Hub](https://hub.docker.com).
- [PostgreSQL docker image details](https://github.com/yizenov/compass_optimizer/blob/master/instructions.pdf)
- [COMPASS docker image details](https://github.com/yizenov/compass_optimizer/blob/master/instructions.pdf)

To replicate the PostgreSQL docker image for runtime comparison, one can follow these [instructions](PostgreSQL_README.md).

## 6. Questions <a name="questions"></a>
If you have questions, please contact:
- Yesdaulet Izenov [yizenov@ucmerced.edu], (https://yizenov.github.io/)
- Asoke Datta [adatta2@ucmerced.edu], (https://asoke26.github.io/adatta2/)
- Brian Tsan [btsan@ucmerced.edu], (https://eecs.ucmerced.edu/content/brian-tsan)
- Florin Rusu [frusu@ucmerced.edu], (https://faculty.ucmerced.edu/frusu/)

## 7. References <a name="references"></a>
<a id="1">[1]</a> [Query optimization through the looking glass, and what we found running the Join Order Benchmark](https://doi.org/10.1007/s00778-017-0480-7)</br>
<a id="2">[2]</a> [COMPASS: Online Sketch-based Query Optimization for In-Memory Databases](https://dl.acm.org/doi/10.1145/3448016.3452840)</br>
<a id="3">[3]</a> [The COMPASS Query Optimizer](https://github.com/yizenov/compass_query_optimizer)</br>
<a id="4">[4]</a> [Filter Push-Down extension for MapD](https://github.com/junhyungshin/mapd-core-fpd)</br>

## 8. Citation <a name="citation"></a>
```bibtex
@misc{l1-error-github,
  author = {Yesdaulet Izenov},
  title = "{L1-error Indicator}",
  howpublished = "\url{https://github.com/yizenov/L1_error_indicator}"
}
```
