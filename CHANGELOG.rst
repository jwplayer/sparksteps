.. :changelog:

Changelog
=========

Releases
--------

v3.0.1 (2020-12-23)
~~~~~~~~~~~~~~~~~~~

* Fixed an issue where `get_bid_price` would always base the instance bid price on the zone with the lowest current instance price, even though the cluster may not be launched in that AZ.

v3.0.0 (2020-08-20)
~~~~~~~~~~~~~~~~~~~

* Fix `determine_best_price` returning a spot price that would be below the current spot price in some conditions.
* Dropped support for Python 3.5.


v2.2.1 (2019-11-04)
~~~~~~~~~~~~~~~~~~~

* Fix `get_demand_price` returning 0.00 for various instance types.


v2.2.0 (2019-09-19)
~~~~~~~~~~~~~~~~~~~

* Support S3 paths in the `uploads` CLI option. A copy step will be added to the EMR cluster which will copy into /home/hadoop from the provided remote path.
* Add option `--service-role` to configure EMR service role beyond the default `EMR_DefaultRole`.


v2.1.0 (2019-08-27)
~~~~~~~~~~~~~~~~~~~

* Add `wait` CLI option. When `--wait` is passed, waits for EMR cluster steps to complete before application exits, sleeping 150 seconds (default) between each poll attempt. An optional integer value can be passed to specify the polling interval to use, in seconds.


v2.0.0 (2019-07-31)
~~~~~~~~~~~~~~~~~~~

* Add `s3-path` CLI argument to optionally configure the path prefix used when writing sparksteps related assets such as sources (file uploads) and logs.

**NOTE:** This is a backwards incompatible change as `sources/` and `logs/` are now written to the location specified by the `s3-path` argument.
Prior to this change logs were written to `s3://S3_BUCKET/logs/sparksteps` and uploads to `s3://S3_BUCKET/sparksteps/sources`.


v1.1.1 (2019-07-22)
~~~~~~~~~~~~~~~~~~~

* Raise an error if one of the file or directory paths provided do not exist


v1.1.0 (2019-07-13)
~~~~~~~~~~~~~~~~~~~

* Add `jobflow_role` CLI argument to configure cluster EC2 Instance Profile
* Add `app-list` CLI argument to configure list of Applications installed on cluster


v1.0.0 (2019-07-03)
~~~~~~~~~~~~~~~~~~~

* Drop support for Python 2
* `defaults` CLI parameter value schema to support arbitrary classifications


v0.4.0 (2017-01-03)
~~~~~~~~~~~~~~~~~~~

* First upload to PyPI.
