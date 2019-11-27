# EECS E6893 Big Data Analytics - Fall 2019

## Final Project: Predicting Grain Growth Performance

```
Due TBD
```

## To run Sphinx Doc Gen
1. Navigate to src/doc and run 'make html'
2. Start Python3 server: python -m http.server
3. Navigate to 'http://localhost:8000/src/doc/_build/html/'

## TL;DR

1. stuff


## Notes (remove before final report)
### Summary
https://automating-gis-processes.github.io/CSC18/lessons/L4/point-in-polygon.html
0. https://pcmdi.llnl.gov/mips/cmip5/usefulDocuments.html
1. VHI: https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/vh_ftp.php
2. LOCA: https://gdo-dcp.ucllnl.org/downscaled_cmip_projections/dcpInterface.html#Projections:%20Complete%20Archives
3. https://marketnews.usda.gov/mnp/ls-report-config
4. US State/County GeoJSON: https://eric.clst.org/tech/usgeojson/
   -- alt1: http://econym.org.uk/gmap/states.xml
   -- alt2: https://github.com/johan/world.geo.json/tree/master/countries/USA
5. https://www.dashingd3js.com/lessons/d3-geo-path
 


### Supervised learning method - Predicive Analytics is what we want...
https://dzone.com/articles/predictive-analytics-with-spark-ml
https://databricks.com/glossary/predictive-analytics
https://www.mathworks.com/discovery/predictive-analytics.html

### NetCDF Python
https://joehamman.com/2013/10/12/plotting-netCDF-data-with-Python/
https://developer.ibm.com/clouddataservices/2016/04/18/predict-temperatures-using-dashdb-python-and-r/

### Models to chose:
https://adaptwest.databasin.org/pages/adaptwest-climatena
1. INM-CM4
2. MPI-ESM-LR
3. GFDL-CM3

### NASA Projection Data
https://cds.nccs.nasa.gov/nex-gddp/

### visualization 
https://www.climate.gov/maps-data/primer/visualizing-climate-data

### Projections
https://gdo-dcp.ucllnl.org/downscaled_cmip_projections/dcpInterface.html#Projections:%20Complete%20Archives

### CMIP 5 vs 3
https://climatechange.environment.nsw.gov.au/Climate-projections-for-NSW/About-NARCliM/CMIP3-vs-CMIP5

### CMIP Data Request
https://gdo-dcp.ucllnl.org/downscaled_cmip_projections/dcpInterface.html#Projections:%20Subset%20Request

### RCPs
https://en.wikipedia.org/wiki/Representative_Concentration_Pathway

### Get vegitation data
Visualizer: https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/vh_browse.php
Data: ftp://ftp.star.nesdis.noaa.gov/pub/corp/scsb/wguo/data/Blended_VH_4km/VH/

__Definitions__
https://www.star.nesdis.noaa.gov/smcd/emb/vci/VH/VH-Syst_10ap30.php
__What is VCI?__
The Vegetation Condition Index (VCI) compares the current NDVI to the range of values observed in the same period in previous years. The VCI is expressed in % and gives an idea where the observed value is situated between the extreme values (minimum and maximum) in the previous years. Lower and higher values indicate bad and good vegetation state conditions, respectively.

__Blended Vegetation Health Product (Blended-VHP)__
Blended-VHP is a re-processed Vegetation Health data set derived from VIIRS (2013-present) and AVHRR (1981-2012) GAC data. It was processed by the newly developed operational VHP system. The new VHP system was improved from GVI-x VH system and some changes/improvement were made to meet the requirement of operation and improve data quality. It can process GAC data from NOAA-19, as well as FRAC data from METOP A and METOP-B. It also produce vegetation health products from VIIRS on NPP and JPSS satellites. VHP system is operationaly running at NOAA Office of Satellite and Product Operations(OSPO) and providing official VHP products. This web site provides recent VH data as a backup/alternative data source. VHP product posted on this VH web site should be consistent to that released by OSPO.   
__Blended-VHP products are saved in 3 NetCDF:__
ND file (*.ND.nc or *.ND.hdf): contains raw NDVI and BT
SM file (*.SM.nc or *.SM.hdf): contains noise removed NDVI and BT
VH file (*.VH.nc or *.VH.hdf): contains VCI,TCI,VHI:
-- Vegetation Condition Index (VCI),
-- Temperature Condition Index(TCI),
-- Vegetation Health Index (VHI)   

## Abstract

The goals of this assignment are to (1) understand how to implement K-means clustering algorithm in Spark by utilizing ​ _transformations and actions_ ​, (2) understand the impact of using different distance measurements and initialization strategies in clustering, (3) learn how to use the built-in Spark MLlib library to conduct supervised and unsupervised learning, (4) have experience of processing data with ​ _ML Pipeline_ and ​ _Dataframe_.

In the first question, you will conduct ​ **document clustering** ​. The dataset we’ll be using is a set of vectorized text documents. In today’s world, you can see applications of document clustering almost everywhere. For example, Flipboard uses LDA topic modelling, approximate nearest neighbor search, and clustering to realize their “similar stories / read more” recommendation feature. You can learn more by reading ​[this blog post](https://engineering.flipboard.com/2017/02/storyclustering)​. To conduct document clustering, you will implement the classic iterative K-means clustering in Spark with different distance functions and initialization strategies.

In the second question, you will load data into Spark Dataframe and perform ​ **binary classification** ​ with Spark MLlib. We will use logistic regression model as our classifier, which is one of the foundational and widely used tools for making classifications. For example, Facebook uses logistic regression as one of the components in its online advertising system. You can read more in a publication ​[here](references/practical-lessons-from-predicting-clicks-on-ads-at-facebook.pdf)​.

## 1. Iterative K-means clustering on Spark


<!-- for Markdown images use this -->
<!--![0](images/-000.png) -->

Note the word ​ _nearest or closest_ ​, we have to define a way to measure it. In class, Prof. Lin mentioned several distance functions. Here, we will use `L2 (Euclidean)` and `L (Manhattan)` distance as our similarity measurement. 

Formally, say we have two points A and B in a d dimensional space, such that A = [a<sub>1</sub>, a<sub>2</sub>, ···, a<sub>d</sub>] and B = [b<sub>1</sub>, b<sub>2</sub>, ···, b<sub>d</sub>], the Euclidean distance (`L2 distance`) between A and B is defined as:


<!--![1](images/-001.png)-->
<div style="text-align: center">
    <img width="300" alt="images/-001.png" src="images/-001.png"/>
</div>

Likewise, the Manhattan distance (L1 distance) between A and B is defined as:

<!--![2](images/-002.png)-->
<div style="text-align: center">
    <img width="300" alt="images/-002.png" src="images/-002.png"/>
</div>

A clustering algorithm aims to minimize ​ _within-cluster cost function_ ​, defined as:

<!--![3](images/-003.png)-->
<div style="text-align: center">
    <img width="300" alt="images/-003.png" src="images/-003.png"/>
    <br/>
    <img width="300" alt="images/-004.png" src="images/-004.png"/>
</div>

for L2 and L1 distance.

Use the data provided to perform document clustering. A document is represented as a 58 dimensional vector of features. Each dimension (or feature) in the vector represents the importance of a word in the document. The idea is that documents with similar sets of word importance may be about the same topic.

The data contains 3 files: (in [q1.zip](data/q1.zip), also on Canvas):

1. _data.txt_ ​ contains the vectorized version of documents, which has 4601 rows and 58 columns.
2. _c1.txt_ ​ contains k initial cluster centroids. These centroids were chosen by
selecting k random points from the input data.
3. _c2.txt_ ​ contains initial cluster centroids which are as far away as possible. You could do this by choosing first centroid c1 randomly, and then finding the point c that is farthest from c1, then selecting c3 which is farthest from c1 and c2, and so on.

For the homework questions, set number of iterations to 20, and the number of clusters k to 10.


### Homework submission for question one: (60%)

Implement iterative K-means in Spark. We’ve provided you with starter code ​ [kmeans.py](data/q1/kmeans.py) on Canvas, which takes care of data loading. Complete the logic inside ​ _for loop_ ​, and run the code with different initialization strategies and loss functions. Feel free to change the code if needed, or paste the code into a Jupyter notebook. Take a screenshot of your code and results in your report.

1. Run clustering on ​ _data.txt_ ​ with​ _c1.txt_ ​ and ​ _c2.txt_ ​ as initial centroids and use L1 distance as similarity measurement. Compute and plot the ​ _within-cluster cost_ ​ for each iteration. You’ll need to submit two graphs here. (20%)

2. Run clustering on ​ _data.txt_ ​ with​ _c1.txt_ ​ and ​ _c2.txt_ ​ as initial centroids and use L2 distance as similarity measurement. Compute and plot the ​ _within-cluster cost_ ​ for each iteration. You’ll need to submit two graphs here. (20%)

3. [t-SNE](https://scikit-learn.org/stable/modules/generated/sklearn.manifold.TSNE.html)​ is a dimensionality reduction method particularly suitable for visualization of high-dimensional data. Visualize your clustering assignment result of (2) by reducing the dimension to a 2D space. You’ll need to submit two graphs here. (10%)

4. For L2 and L1 distance, are random initialization of K-means using ​ _c1.txt_ ​ better than initialization using ​ _c2.txt_ ​ in terms of cost? Explain your reasoning. (5%)

6. What is the time complexity of the iterative K-means? (5%)


## 2. Binary classification with Spark MLlib
In this question, we are going to use​ [the Adult dataset](https://archive.ics.uci.edu/ml/datasets/Adult)​, which is publicly available at the [UCI Machine Learning Repository](https://archive.ics.uci.edu/ml/index.php)​. You can download the ​ _adult.data_ ​ [from this link](https://archive.ics.uci.edu/ml/machine-learning-databases/adult/)​, and add .csv extension to the downloaded file.

This data derives from census data, and consists of information about 48k individuals and their annual income. We are interested in using the information to predict if an individual earns less than 50K or larger than 50k per year. It is natural to formulate the problem into binary classification.

The model we use for binary classification is logistic regression, which is a classification model (don’t be fooled by its name). It takes one more step than linear regression: apply the sigmoid function to compress the output values to 0~1 and do the threshold. The outputs of linear regression are continuous number values, while the outputs of logistic regression are probability values which can then be mapped to two or more discrete classes. You could learn more details like how do Spark perform optimization ​[here](https://spark.apache.org/docs/latest/mllib-linear-methods.html)​.

### Homework submission for question two: (40%)

Upload the csv file to GCP cloud storage, and finish the following steps. Provide screenshots to your code and results. Feel free to do EDA, feature engineering, or try other more complicated models.

1. Data loading: (10%)
    * Read the csv file into a Dataframe.
    * You could set ​ _"inferschema"_ ​ to true and rename the columns with the following information: "age", "workclass", "fnlwgt","education", "education_num", "marital_status", "occupation", "relationship", "race", "sex", "capital_gain", "capital_loss", "hours_per_week", "native_country", "income". * You could learn more about Dataframe from​ this [doc](https://spark.apache.org/docs/latest/sql-getting-started.html)​.

2. Data preprocessing: (10%)
    * Convert the categorical variables into numeric variables with ​[ML Pipelines](https://spark.apache.org/docs/latest/ml-pipeline.html)​ and [Feature Transformers​](https://spark.apache.org/docs/latest/ml-features.html).
    * You will probably need ​ _OneHotEncoderEstimator,_
    _StringIndexer, and VectorAssembler._ ​
    * Split your data into training set and test set with ratio of 70% and 30% and set the seed to 100.

3. Modelling: (10%)
    * Train a ​[logistic regression model from Spark MLlib](https://spark.apache.org/docs/latest/ml-classification-regression.html)​ with train set.
    * After training, plot ROC curve and Precision-Recall curve of your training process.

4. Evaluation: (10%)
    * Apply your trained model on the test set. 
    * Provide the value of area under ROC, accuracy, and confusion matrix.
    * You should expect the accuracy to be around 85%.
