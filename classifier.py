import pyspark
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.tree import DecisionTree
from pyspark import SparkContext
from pyspark.sql import SQLContext

##If running through pyspark, start pyspark as follows: pyspark --packages com.databricks:spark-csv_2.10:1.2.0
sc = pyspark.SparkContext()
sqlContext = SQLContext(sc)
#We point the context at a CSV file on disk. The result is a RDD, not the content of the file. This is a Spark transformation

raw_rdd = sc.textFile("/tmp/titanic.csv")
# We need to make the necessary installs to allow the following line to function (create dataframe from CSV file)
#df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load('/tmp/titanic.csv')

We query RDD for the number of lines in the file. The call here causes the file to be read and the result computed. This is a Spark action

raw_rdd.count()

#We query for the first five rows of the RDD. Even though the data is small, we shouldn't get into the habit of pulling the entire dataset into the notebook. Many datasets that we might want to work with using Spark will be much too large to fit in memory of a single machine.

raw_rdd.take(5)

#We see a header row followed by a set of data rows. We filter out the header to define a new RDD containing only the data rows.
header = raw_rdd.first()
data_rdd = raw_rdd.filter(lambda line: line != header)

#We take a random sample of the data rows to better understand the possible values.
data_rdd.takeSample(False, 5, 0)


#We see that the first value in every row is a passenger number. The next three values are the passenger attributes we might use to predict passenger survival: ticket class, age group, and gender. The final value is the survival ground truth.

#Create labeled points (i.e., feature vectors and ground truth)
#Now we define a function to turn the passenger attributions into structured LabeledPoint objects

def row_to_labeled_point(line):
    passenger_id, klass, age, sex, survived = [segs.strip('"') for segs in line.split(',')]
    klass = int(klass[0]) - 1
    
    if (age not in ['adults', 'child'] or 
        sex not in ['man', 'women'] or
        survived not in ['yes', 'no']):
        raise RuntimeError('unknown value')
    
    features = [
        klass,
        (1 if age == 'adults' else 0),
        (1 if sex == 'women' else 0)
    ]
    return LabeledPoint(1 if survived == 'yes' else 0, features)

#We apply the function to all rows.
labeled_points_rdd = data_rdd.map(row_to_labeled_point)

#We take a random sample of the resulting points to inspect them.
labeled_points_rdd.takeSample(False, 5, 0)

#We split the transformed data into a training (70%) and test set (30%), and print the total number of items in each segment.
training_rdd, test_rdd = labeled_points_rdd.randomSplit([0.7, 0.3], seed = 0)

training_count = training_rdd.count()
test_count = test_rdd.count()
training_count, test_count

#Now we train a DecisionTree model. We specify that we're training a boolean classifier (i.e., there are two outcomes). We also specify that all of our features are categorical and the number of possible categories for each.
model = DecisionTree.trainClassifier(training_rdd, numClasses=2, categoricalFeaturesInfo={0: 3,1: 2,2: 2})


#We now apply the trained model to the feature values in the test set to get the list of predicted outcomines.

predictions_rdd = model.predict(test_rdd.map(lambda x: x.features))


#We bundle our predictions with the ground truth outcome for each passenger in the test set.
truth_and_predictions_rdd = test_rdd.map(lambda lp: lp.label).zip(predictions_rdd)

#Now we compute the test error (% predicted survival outcomes == actual outcomes) and display the decision tree for good measure
accuracy = truth_and_predictions_rdd.filter(lambda v_p: v_p[0] == v_p[1]).count() / float(test_count)
print('Accuracy =', accuracy)
print(model.toDebugString())

#For a simple comparison, we also train and test a LogisticRegressionWithSGD model
model = LogisticRegressionWithSGD.train(training_rdd)

predictions_rdd = model.predict(test_rdd.map(lambda x: x.features))

labels_and_predictions_rdd = test_rdd.map(lambda lp: lp.label).zip(predictions_rdd)

accuracy = labels_and_predictions_rdd.filter(lambda v_p: v_p[0] == v_p[1]).count() / float(test_count)

print('Accuracy =', accuracy)