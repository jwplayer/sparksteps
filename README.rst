Spark Steps
===========

.. image:: https://travis-ci.org/jwplayer/sparksteps.svg?branch=master
    :target: https://travis-ci.org/jwplayer/sparksteps
    :alt: Build Status

.. image:: https://readthedocs.org/projects/spark-steps/badge/?version=latest
    :target: http://spark-steps.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status

SparkSteps allows you to configure your EMR cluster and upload your
spark script and its dependencies via AWS S3. All you need to do is
define an S3 bucket.

Install
-------

::

    pip install sparksteps

CLI Options
-----------

::

    Prompt parameters:
      app               main spark script for submit spark (required)
      app-args:         arguments passed to main spark script
      aws-region:       AWS region name
      bid-price:        specify bid price for task nodes
      cluster-id:       job flow id of existing cluster to submit to
      debug:            allow debugging of cluster
      dynamic-pricing:  allow sparksteps to determine best bid price for task nodes
      ec2-key:          name of the Amazon EC2 key pair
      ec2-subnet-id:    Amazon VPC subnet id
      help (-h):        argparse help
      keep-alive:       Keep EMR cluster alive when no steps
      master:           instance type of of master host (default='m4.large')
      name:             specify cluster name
      num-core:         number of core nodes
      num-task:         number of task nodes
      release-label:    EMR release label
      s3-bucket:        name of s3 bucket to upload spark file (required)
      s3-dist-cp:       s3-dist-cp step after spark job is done
      slave:            instance type of of slave hosts
      submit-args:      arguments passed to spark-submit
      sparksteps-conf:  use sparksteps Spark conf
      tags:             EMR cluster tags of the form "key1=value1 key2=value2"
      uploads:          files to upload to /home/hadoop/ in master instance

Example
-------

::

      AWS_S3_BUCKET = <insert-s3-bucket>
      cd sparksteps/
      sparksteps examples/episodes.py \
        --s3-bucket $AWS_S3_BUCKET \
        --aws-region us-east-1 \
        --release-label emr-4.7.0 \
        --uploads examples/lib examples/episodes.avro \
        --submit-args="--deploy-mode client --jars /home/hadoop/lib/spark-avro_2.10-2.0.2-custom.jar" \
        --app-args="--input /home/hadoop/episodes.avro" \
        --tags Application="Spark Steps" \
        --debug

The above example creates an EMR cluster of 1 node with default instance
type *m4.large*, uploads the pyspark script episodes.py and its
dependencies to the specified S3 bucket and copies the file from S3 to
the cluster. Each operation is defined as an EMR "step" that you can
monitor in EMR. The final step is to run the spark application with
submit args that includes a custom spark-avro package and app args
"--input".

Run Spark Job on Existing Cluster
---------------------------------

You can use the option ``--cluster-id`` to specify a cluster to upload
and run the Spark job. This is especially helpful for debugging.

Dynamic Pricing (alpha)
-----------------------

Use CLI option ``--dynamic-pricing`` to allow sparksteps to dynamically
determine best bid price for EMR task notes.

Currently the algorithm looks back at spot history over the last 12
hours and calculates ``min(50% * on_demand_price, max_spot_price)`` to
determine bid price. That said, if the current spot price is over 80% of
the on-demand cost, then on-demand instances are used to be
conservative.

Note: code depends on `ec2instances <http://www.ec2instances.info/>`__
for getting demand price.

Testing
-------

::

    make test

License
-------

Apache License 2.0
