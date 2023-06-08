from constructs import Construct
from pathlib import Path
from datalake_cdk.storage import bucket, objects, assets
from datalake_cdk.etl import job, script, iam
from datalake_lib.etl import arguments, options
from datalake_lib.etl.iam import BucketAccessConfiguration
from datalake_lib.etl.job import JobProperties
from datalake_lib import python
import typing


class HudiJobConstruct(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        glue_bucket: bucket.CdkBucket,
        job_script_filename: str,
        source_bucket_access_configurations: typing.Sequence[BucketAccessConfiguration],
        target_bucket_access_configurations: typing.Sequence[BucketAccessConfiguration],
        job_arguments: typing.Optional[typing.Dict[str, str]] = None,
    ) -> None:
        super().__init__(scope, id)

        job_arguments = {} if not job_arguments else job_arguments

        job_script_path = Path("job_scripts") / job_script_filename
        job_script_file = objects.CdkBucketObject(
            assets.CdkAsset(f"{id}-asset", job_script_path),
            bucket=glue_bucket,
            object_key=Path("scripts"),
        )
        job_script = script.CdkJobScript(job_script_file, python.Version.PYTHON3_10)

        glue_role = iam.CdkGlueJobRole(
            self,
            id + "-role",
            bucket_read_access_configurations=[
                BucketAccessConfiguration(glue_bucket.bucket_arn),
                *source_bucket_access_configurations,
            ],
            bucket_write_access_configurations=[*target_bucket_access_configurations],
        )
        job_args = [
            arguments.DatalakeFormatArgument([options.DatalakeFormat.HUDI]),
            arguments.EnableContinuousCloudwatchLog(
                enable=True, filter_apache_logs=True
            ),
            arguments.EnableGlueCatalog(True),
            arguments.PythonPackageDependencies(
                pip_installable_packages=[
                    arguments.PipInstallablePackage(
                        "glue-helper-lib",
                        version_specifier=arguments.VersionSpecifier.MATCH,
                        version_value="0.4.0",
                    )
                ],
            ),
        ]

        custom_job_args = [
            arguments.CustomDefaultArgument(arg_name, arg_value)
            for arg_name, arg_value in job_arguments.items()
        ]

        job.Job(
            self,
            "job",
            JobProperties(
                script=job_script,
                iam_role=glue_role,
                max_retries=0,
                default_arguments=job_args + custom_job_args,
            ),
        )
