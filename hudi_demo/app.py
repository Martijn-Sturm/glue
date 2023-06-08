#!/usr/bin/env python3
import aws_cdk as cdk
from datalake_cdk.storage import bucket
from datalake_lib.etl.iam import BucketAccessConfiguration
import job


class DemoHudiUpsertStack(cdk.Stack):
    def __init__(self, scope, id, **kwargs):
        super().__init__(scope, id, **kwargs)

        glue_bucket = bucket.CdkBucket(self, "glue-assets")

        target_bucket = bucket.CdkBucket(self, "target-bucket")

        job_arguments = {
            "database_name": "demodb",
            "table_name": "demo_table",
            "storage_path": f"{target_bucket.s3_url_for_object()}/demo_table",
        }

        job.HudiJobConstruct(
            self,
            "create-table",
            glue_bucket=glue_bucket,
            job_script_filename="create_table.py",
            source_bucket_access_configurations=[],
            target_bucket_access_configurations=[
                BucketAccessConfiguration(target_bucket.bucket_arn)
            ],
            job_arguments=job_arguments,
        )

        # job.HudiJobConstruct(
        #     self,
        #     "upsert-table",
        #     glue_bucket=glue_bucket,
        #     job_script_filename="upsert_table.py",
        #     source_bucket_access_configurations=[],
        #     target_bucket_access_configurations=[
        #         BucketAccessConfiguration(target_bucket.bucket_arn)
        #     ],
        #     job_arguments=job_arguments,
        # )

        # local_lib_wheel_path = Path(
        #     "libs/demo_lib/dist/demo_lib-0.1.0-py3-none-any.whl"
        # )
        # demo_lib = objects.CdkBucketObject(
        #     assets.CdkAsset(str(Path("demo_lib")), local_lib_wheel_path),
        #     bucket=glue_bucket,
        #     object_key=Path("wheels"),
        # )


app = cdk.App()
DemoHudiUpsertStack(
    app,
    "DemoHudiUpsertStack",
    env=cdk.Environment(region="eu-central-1"),
)

app.synth()
