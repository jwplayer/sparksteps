# -*- coding: utf-8 -*-
"""Create EMR steps and upload files."""
import os
import tempfile
import zipfile

REMOTE_DIR = '/home/hadoop/'
S3_KEY_PREFIX = 'sparksteps/sources/'
S3_URI_FMT = "s3://{bucket}/{key}"


def get_basename(path):
    return os.path.basename(os.path.normpath(path))


def ls_recursive(dirname):
    """Recursively list files in a directory."""
    for (dirpath, dirnames, filenames) in os.walk(os.path.expanduser(dirname)):
        for f in filenames:
            yield os.path.join(dirpath, f)


def zip_to_s3(s3_resource, dirpath, bucket, key):
    """Zip folder and upload to S3."""
    with tempfile.SpooledTemporaryFile() as tmp:
        with zipfile.ZipFile(tmp, 'w', zipfile.ZIP_DEFLATED) as archive:
            for fpath in ls_recursive(dirpath):
                archive.write(fpath, get_basename(fpath))
        tmp.seek(0)  # Reset file pointer
        response = s3_resource.Bucket(bucket).put_object(Key=key, Body=tmp)
    return response


class CmdStep(object):
    on_failure = 'CANCEL_AND_WAIT'

    @property
    def step_name(self):
        raise NotImplementedError()

    @property
    def cmd(self):
        raise NotImplementedError()

    @property
    def step(self):
        return {
            'Name': self.step_name,
            'ActionOnFailure': self.on_failure,
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': self.cmd
            }
        }


class CopyStep(CmdStep):
    def __init__(self, bucket, filename):
        self.bucket = bucket
        self.filename = filename

    @property
    def step_name(self):
        return "Copy {}".format(self.filename)

    @property
    def cmd(self):
        return ['aws', 's3', 'cp', self.s3_uri, REMOTE_DIR]

    @property
    def key(self):
        return S3_KEY_PREFIX + self.filename

    @property
    def s3_uri(self):
        return S3_URI_FMT.format(bucket=self.bucket, key=self.key)


class DebugStep(CmdStep):
    on_failure = 'TERMINATE_CLUSTER'

    @property
    def step_name(self):
        return "Setup - debug"

    @property
    def cmd(self):
        return ['state-pusher-script']


class SparkStep(CmdStep):
    def __init__(self, app_path, submit_args=None, app_args=None):
        self.app = get_basename(app_path)
        self.submit_args = submit_args or []
        self.app_args = app_args or []

    @property
    def step_name(self):
        return "Run {}".format(self.app)

    @property
    def cmd(self):
        return (['spark-submit'] + self.submit_args + [self.remote_app] +
                self.app_args)

    @property
    def remote_app(self):
        return os.path.join(REMOTE_DIR, self.app)


class UnzipStep(CmdStep):
    def __init__(self, dirpath):
        self.dirpath = dirpath

    @property
    def step_name(self):
        return "Unzip {}".format(self.zipfile)

    @property
    def cmd(self):
        return ['unzip', '-o', self.remote_zipfile, '-d', self.remote_dirpath]

    @property
    def zipfile(self):
        return self.dirname + '.zip'

    @property
    def remote_zipfile(self):
        return os.path.join(REMOTE_DIR, self.zipfile)

    @property
    def dirname(self):
        return get_basename(self.dirpath)

    @property
    def remote_dirpath(self):
        return os.path.join(REMOTE_DIR, self.dirname)


class S3DistCp(CmdStep):
    on_failure = 'CONTINUE'

    def __init__(self, s3_dist_cp):
        self.s3_dist_cp = s3_dist_cp

    @property
    def step_name(self):
        return "S3DistCp step"

    @property
    def cmd(self):
        return ['s3-dist-cp'] + self.s3_dist_cp


def upload_steps(s3_resource, bucket, path):
    """Upload files to S3 and get steps."""
    steps = []
    basename = get_basename(path)
    if os.path.isdir(path):  # zip directory
        copy_step = CopyStep(bucket, basename + '.zip')
        zip_to_s3(s3_resource, path, bucket, key=copy_step.key)
        steps.extend([copy_step, UnzipStep(path)])
    else:
        copy_step = CopyStep(bucket, basename)
        s3_resource.meta.client.upload_file(path, bucket, copy_step.key)
        steps.append(copy_step)
    return steps


def setup_steps(s3, bucket, app_path, submit_args=None, app_args=None,
                uploads=None, s3_dist_cp=None):
    cmd_steps = []
    paths = uploads or []
    paths.append(app_path)

    for path in paths:
        cmd_steps.extend(upload_steps(s3, bucket, path))

    cmd_steps.append(SparkStep(app_path, submit_args, app_args))

    if s3_dist_cp is not None:
        cmd_steps.append(S3DistCp(s3_dist_cp))

    return [s.step for s in cmd_steps]
