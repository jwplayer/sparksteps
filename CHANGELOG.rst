.. :changelog:

Changelog
=========

Releases
--------

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
