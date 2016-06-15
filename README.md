# sparksteps

Sparksteps allows you to configure your EMR cluster and upload your
spark script and its dependencies via AWS S3. All you need to do is
define an S3 bucket.

## Install

We aim to get the project on PyPI but for now you can just run:

```
git clone https://github.com/jwplayer/sparksteps.git
cd sparksteps/
python setup.py install
```

## CLI Options

```
Prompt parameters:
  app               main spark script for submit spark (required)
  app-args:         arguments passed to main spark script
  aws-region:       AWS region name (required)
  cluster-id:       job flow id of existing cluster to submit to
  conf-file:        specify cluster config file
  ec2-key:          name of the Amazon EC2 key pair to use when using SSH
  ec2-subnet-id:    Amazon VPC subnet id
  help (-h):        argparse help
  keep-alive:       Keep EMR cluster alive when no steps
  master:           instance type of of master host (default='m4.large')
  num-nodes:        number of instances (default=1)
  release-label:    EMR release label (required)
  s3-bucket:        name of s3 bucket to upload spark file (required)
  slave:            instance type of of slave hosts (default='m4.2xlarge')
  submit-args:      arguments passed to spark-submit
  sparksteps-conf:  use sparksteps Spark conf
  tags:             EMR cluster tags of the form "key1=value1 key2=value2"
  uploads:          directories to upload to master instance in /home/hadoop/
```

## Example

```
  AWS_S3_BUCKET = <insert-s3-bucket>
  cd sparksteps/
  sparksteps examples/episodes.py \
    --s3-bucket $AWS_S3_BUCKET \
    --aws-region us-east-1 \
    --release-label emr-4.7.0 \
    --uploads examples/lib examples/episodes.avro \
    --submit-args="--deploy-mode client --jars /home/hadoop/lib/spark-avro_2.10-2.0.2-custom.jar" \
    --app-args="--input /home/hadoop/episodes.avro" \
    --num-nodes 1 \
    --tags Application="Spark Steps" \
    --conf-file examples/cluster.json \
    --debug
```

The above example creates an EMR cluster of 1 node with default instance
type _m4.large_, uploads the pyspark script episodes.py to the specified
S3 bucket and copies the file from S3 to the cluster. Each operation is defined
as an EMR “step” that you can monitor in EMR. The final step is to run the spark
application with submit args that includes a custom spark-avro package and
app args “--input”.

## Run Spark Job on Existing Cluster

You can use the option `--cluster-id` to specify a cluster to upload 
and run the Spark job. This is especially helpful for debugging.

## Specify Configuration

To override the default configurations, just use the `--conf-file` option. 
See examples/cluster.json for a detailed example. The formatting of options
follows boto3 [run job flow](http://boto3.readthedocs.io/en/latest/reference/services/emr.html#EMR.Client.run_job_flow).
Note you only need to specify properties you want to override as opposed to
providing an entire configuration.

## Testing

```
pip install -r requirements-test.txt
py.test sparksteps/tests.py
```

## Known Issues

If a conf file is specified, its parameters will overwrite anything specified
in the command line arguments.

## License

Apache License 2.0
