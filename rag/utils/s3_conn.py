#
#  Copyright 2025 The InfiniFlow Authors. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

import logging
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
import time
import mimetypes
from io import BytesIO
from common.decorator import singleton
from common import settings



@singleton
class RAGFlowS3:
    def __init__(self):
        self.conn = None
        self.gcs_client = None
        self.gcs_bucket_name = None
        self.s3_config = settings.S3
        self.access_key = self.s3_config.get("access_key", None)
        self.secret_key = self.s3_config.get("secret_key", None)
        self.session_token = self.s3_config.get("session_token", None)
        self.region = self.s3_config.get("region", None)
        self.endpoint_url = self.s3_config.get("endpoint_url", None)
        self.signature_version = self.s3_config.get("signature_version", None)
        self.addressing_style = self.s3_config.get("addressing_style", None)
        self.bucket = self.s3_config.get("bucket", None)
        self.prefix_path = self.s3_config.get("prefix_path", None)
        self.__open__()

    @staticmethod
    def use_default_bucket(method):
        def wrapper(self, bucket, *args, **kwargs):
            # If there is a default bucket, use the default bucket
            actual_bucket = self.bucket if self.bucket else bucket
            return method(self, actual_bucket, *args, **kwargs)

        return wrapper

    @staticmethod
    def use_prefix_path(method):
        def wrapper(self, bucket, fnm, *args, **kwargs):
            # If the prefix path is set, use the prefix path.
            # The bucket passed from the upstream call is
            # used as the file prefix. This is especially useful when you're using the default bucket
            if self.prefix_path:
                fnm = f"{self.prefix_path}/{bucket}/{fnm}"
            return method(self, bucket, fnm, *args, **kwargs)

        return wrapper

    def __open__(self):
        try:
            if self.conn:
                self.__close__()
        except Exception:
            pass

        try:
            s3_params = {}
            config_kwargs = {}

            # if not set ak/sk, boto3 s3 client would try several ways to do the authentication
            # see doc: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials
            is_gcs = self.endpoint_url and "storage.googleapis.com" in self.endpoint_url

            if self.access_key and self.secret_key:
                s3_params = {
                    "aws_access_key_id": self.access_key,
                    "aws_secret_access_key": self.secret_key,
                    "aws_session_token": self.session_token,
                }
            elif is_gcs:
                # For GCS, try to get credentials from Google Application Default Credentials
                try:
                    from google.auth import default
                    import google.auth.transport.requests

                    credentials, project = default(
                        scopes=["https://www.googleapis.com/auth/cloud-platform"]
                    )
                    request = google.auth.transport.requests.Request()
                    credentials.refresh(request)

                    # Use GOOG1E as access key ID with the OAuth token for GCS S3-compatible API
                    s3_params = {
                        "aws_access_key_id": "GOOG1E",
                        "aws_secret_access_key": credentials.token,
                    }
                    logging.info(
                        "Using Google Cloud Storage with Application Default Credentials"
                    )

                    # Also initialize native GCS client for operations that might fail with S3 API
                    try:
                        from google.cloud import storage

                        self.gcs_client = storage.Client(project=project or "toteqa")
                        self.gcs_bucket_name = self.bucket
                        logging.info(
                            "Native GCS client initialized for reliable operations"
                        )
                    except ImportError:
                        logging.warning(
                            "google-cloud-storage not available, S3-compatible mode only"
                        )
                        self.gcs_client = None

                except Exception as e:
                    logging.warning(f"Failed to get Google credentials: {e}")
                    self.gcs_client = None
            # For other services without explicit keys, let boto3 use Application Default Credentials

            if self.region:
                s3_params["region_name"] = self.region
            if self.endpoint_url:
                s3_params["endpoint_url"] = self.endpoint_url

            # Configure signature_version and addressing_style through Config object
            if self.signature_version:
                config_kwargs["signature_version"] = self.signature_version
            if self.addressing_style:
                config_kwargs["s3"] = {"addressing_style": self.addressing_style}

            if config_kwargs:
                s3_params["config"] = Config(**config_kwargs)

            self.conn = boto3.client("s3", **s3_params)

            # Log which service we're connecting to
            if self.endpoint_url and "storage.googleapis.com" in self.endpoint_url:
                logging.info("Connecting to Google Cloud Storage via S3-compatible API")
            else:
                logging.info("Connecting to S3-compatible storage")

        except Exception:
            logging.exception(
                f"Fail to connect at region {self.region} or endpoint {self.endpoint_url}"
            )

    def __close__(self):
        del self.conn
        self.conn = None
        self.gcs_client = None

    @use_default_bucket
    def bucket_exists(self, bucket, *args, **kwargs):
        try:
            logging.debug(f"head_bucket bucketname {bucket}")
            self.conn.head_bucket(Bucket=bucket)
            return True
        except ClientError as e:
            logging.exception(f"head_bucket error {bucket}")

            # Try GCS native client as fallback
            if self.gcs_client:
                try:
                    logging.info(f"Trying GCS native client to check bucket {bucket}")
                    gcs_bucket = self.gcs_client.bucket(bucket)
                    # Try to get bucket metadata to check if it exists
                    gcs_bucket.reload()
                    logging.info(
                        f"Bucket {bucket} exists (confirmed via GCS native client)"
                    )
                    return True
                except Exception as gcs_e:
                    logging.info(
                        f"Bucket {bucket} does not exist or not accessible via GCS: {gcs_e}"
                    )

                    # Check if it's a billing/account error (403) - these are fatal for GCS
                    if hasattr(gcs_e, "code") and gcs_e.code == 403:
                        logging.error(f"GCS account/billing issue detected: {gcs_e}")
                        logging.error(
                            "Please check your Google Cloud billing account and project settings"
                        )

                    return False

            return False

    def health(self):
        """Test S3/GCS connectivity by uploading and then removing a test file"""
        bucket = self.bucket
        test_filename = f"health_test_{int(time.time())}.txt"
        test_content = b"health_check_test_content"

        if self.prefix_path:
            test_filename = f"{self.prefix_path}/{test_filename}"

        try:
            # Check if bucket exists, create if it doesn't
            if not self.bucket_exists(bucket):
                try:
                    self.conn.create_bucket(Bucket=bucket)
                    logging.info(f"Created bucket {bucket} during health check")
                except Exception as bucket_create_error:
                    logging.warning(
                        f"Health check: S3 API bucket creation failed for {bucket}: {bucket_create_error}"
                    )

                    # Try GCS native client for bucket creation if available
                    if self.gcs_client:
                        try:
                            logging.info(
                                f"Health check: Trying GCS native client to create bucket {bucket}"
                            )
                            gcs_bucket = self.gcs_client.bucket(bucket)
                            gcs_bucket.create()
                            logging.info(
                                f"Health check: Successfully created bucket {bucket} via GCS native client"
                            )
                        except Exception as gcs_bucket_error:
                            logging.warning(
                                f"Health check: GCS native bucket creation also failed: {gcs_bucket_error}"
                            )
                            # Continue anyway - bucket might already exist
                    else:
                        logging.warning(
                            "Health check: No GCS native client available for bucket creation fallback"
                        )

            # Test upload
            try:
                self.conn.upload_fileobj(BytesIO(test_content), bucket, test_filename)
                logging.debug(
                    f"Health check: successfully uploaded test file {test_filename}"
                )
            except Exception as e:
                logging.error(f"Health check: failed to upload test file: {e}")

                # Try GCS native client as fallback
                if self.gcs_client:
                    try:
                        gcs_bucket = self.gcs_client.bucket(bucket)
                        blob = gcs_bucket.blob(test_filename)
                        # Use upload_from_file with BytesIO for binary data
                        blob.upload_from_file(
                            BytesIO(test_content), content_type="text/plain"
                        )
                        logging.info(
                            "Health check: successfully uploaded via GCS native client"
                        )
                    except Exception as gcs_e:
                        logging.error(
                            f"Health check: GCS native upload also failed: {gcs_e}"
                        )

                        # Check if it's a billing/account error (403) - these are fatal for GCS
                        if hasattr(gcs_e, "code") and gcs_e.code == 403:
                            logging.error(
                                f"Health check: GCS account/billing issue detected: {gcs_e}"
                            )
                            logging.error(
                                "Health check: Please check your Google Cloud billing account and project settings"
                            )

                        return False
                else:
                    return False

            # Test download to verify upload worked
            try:
                response = self.conn.get_object(Bucket=bucket, Key=test_filename)
                downloaded_content = response["Body"].read()
                if downloaded_content != test_content:
                    logging.error(
                        "Health check: downloaded content doesn't match uploaded content"
                    )
                    return False
                logging.debug(
                    "Health check: successfully verified upload by downloading"
                )
            except Exception as e:
                logging.error(f"Health check: failed to download test file: {e}")

                # Try GCS native client as fallback
                if self.gcs_client:
                    try:
                        gcs_bucket = self.gcs_client.bucket(bucket)
                        blob = gcs_bucket.blob(test_filename)
                        downloaded_content = blob.download_as_bytes()
                        if downloaded_content != test_content:
                            logging.error(
                                "Health check: GCS downloaded content doesn't match"
                            )
                            return False
                        logging.info(
                            "Health check: successfully verified via GCS native client"
                        )
                    except Exception as gcs_e:
                        logging.error(
                            f"Health check: GCS native download also failed: {gcs_e}"
                        )
                        return False
                else:
                    return False

            # Clean up test file
            try:
                self.conn.delete_object(Bucket=bucket, Key=test_filename)
                logging.debug(f"Health check: cleaned up test file {test_filename}")
            except Exception as e:
                logging.warning(
                    f"Health check: failed to clean up test file {test_filename}: {e}"
                )

                # Try GCS native client for cleanup
                if self.gcs_client:
                    try:
                        gcs_bucket = self.gcs_client.bucket(bucket)
                        blob = gcs_bucket.blob(test_filename)
                        blob.delete()
                        logging.info(
                            "Health check: cleaned up test file via GCS native client"
                        )
                    except Exception as gcs_e:
                        logging.warning(
                            f"Health check: GCS native cleanup also failed: {gcs_e}"
                        )

            logging.info("Health check: passed successfully")
            return True

        except Exception as e:
            logging.error(f"Health check: unexpected error: {e}")
            return False

    def get_properties(self, bucket, key):
        return {}

    def list(self, bucket, dir, recursive=True):
        return []

    @use_prefix_path
    @use_default_bucket
    def put(self, bucket, fnm, binary, *args, **kwargs):
        logging.debug(f"bucket name {bucket}; filename :{fnm}:")

        # Determine content type based on file extension
        content_type = self._get_content_type(fnm)
        logging.debug(f"Determined content type for {fnm}: {content_type}")

        # For GCS endpoints, prioritize native GCS client over S3-compatible API
        is_gcs = self.endpoint_url and "storage.googleapis.com" in self.endpoint_url

        if is_gcs and self.gcs_client:
            # Try GCS native client first for GCS endpoints
            try:
                logging.info(f"Using GCS native client for put {bucket}/{fnm}")

                # Ensure bucket exists
                if not self.bucket_exists(bucket):
                    try:
                        gcs_bucket = self.gcs_client.bucket(bucket)
                        gcs_bucket.create()
                        logging.info(f"Created bucket {bucket} via GCS native client")
                    except Exception as bucket_create_error:
                        logging.warning(
                            f"GCS bucket creation failed: {bucket_create_error}"
                        )
                        # Continue anyway - bucket might already exist

                # Upload file using GCS native client
                gcs_bucket = self.gcs_client.bucket(bucket)
                blob = gcs_bucket.blob(fnm)
                blob.upload_from_file(BytesIO(binary), content_type=content_type)
                logging.info(
                    f"Successfully uploaded {bucket}/{fnm} via GCS native client with content-type: {content_type}"
                )
                return True

            except Exception as gcs_e:
                logging.warning(f"GCS native upload failed for {bucket}/{fnm}: {gcs_e}")
                # Fall back to S3-compatible API
                logging.info("Falling back to S3-compatible API")

        # Use S3-compatible API (either as primary for non-GCS or as fallback for GCS)
        for _ in range(1):
            try:
                if not self.bucket_exists(bucket):
                    try:
                        self.conn.create_bucket(Bucket=bucket)
                        logging.info(f"create bucket {bucket} ********")
                    except Exception as bucket_create_error:
                        logging.warning(
                            f"S3 API bucket creation failed for {bucket}: {bucket_create_error}"
                        )
                        # Continue anyway - bucket might already exist

                # Upload with content type
                extra_args = {"ContentType": content_type}
                r = self.conn.upload_fileobj(
                    BytesIO(binary), bucket, fnm, ExtraArgs=extra_args
                )
                return r

            except Exception as e:
                logging.exception(f"S3 API put failed for {bucket}/{fnm}: {e}")

                # If this is GCS and we haven't tried native client yet, try it now
                if is_gcs and self.gcs_client:
                    try:
                        logging.info(
                            f"Trying GCS native client as final fallback for put {bucket}/{fnm}"
                        )
                        gcs_bucket = self.gcs_client.bucket(bucket)
                        blob = gcs_bucket.blob(fnm)
                        blob.upload_from_file(
                            BytesIO(binary), content_type=content_type
                        )
                        logging.info(
                            f"Successfully uploaded {bucket}/{fnm} via GCS native client fallback with content-type: {content_type}"
                        )
                        return True
                    except Exception as gcs_e:
                        logging.exception(
                            f"GCS native fallback also failed for {bucket}/{fnm}: {gcs_e}"
                        )

                # Both methods failed, raise the original error
                self.__open__()
                time.sleep(1)
                raise e

    @use_prefix_path
    @use_default_bucket
    def rm(self, bucket, fnm, *args, **kwargs):
        try:
            self.conn.delete_object(Bucket=bucket, Key=fnm)
        except Exception as e:
            logging.exception(f"S3 API rm failed for {bucket}/{fnm}: {e}")

            # Try GCS native client as fallback
            if self.gcs_client:
                try:
                    logging.info(f"Trying GCS native client to delete {bucket}/{fnm}")
                    gcs_bucket = self.gcs_client.bucket(bucket)
                    blob = gcs_bucket.blob(fnm)
                    blob.delete()
                    logging.info(
                        f"Successfully deleted {bucket}/{fnm} via GCS native client"
                    )
                except Exception as gcs_e:
                    logging.exception(
                        f"GCS native rm also failed for {bucket}/{fnm}: {gcs_e}"
                    )
            else:
                logging.exception(f"Fail rm {bucket}/{fnm}")

    @use_prefix_path
    @use_default_bucket
    def get(self, bucket, fnm, *args, **kwargs):
        for _ in range(1):
            try:
                r = self.conn.get_object(Bucket=bucket, Key=fnm)
                object_data = r["Body"].read()
                return object_data
            except Exception as e:
                logging.exception(f"S3 API get failed for {bucket}/{fnm}: {e}")

                # Try GCS native client as fallback
                if self.gcs_client:
                    try:
                        logging.info(f"Trying GCS native client for get {bucket}/{fnm}")
                        gcs_bucket = self.gcs_client.bucket(bucket)
                        blob = gcs_bucket.blob(fnm)
                        data = blob.download_as_bytes()
                        logging.info(
                            f"Successfully downloaded {bucket}/{fnm} via GCS native client"
                        )
                        return data
                    except Exception as gcs_e:
                        logging.exception(
                            f"GCS native get also failed for {bucket}/{fnm}: {gcs_e}"
                        )

                self.__open__()
                time.sleep(1)
                raise e
        return None

    @use_prefix_path
    @use_default_bucket
    def obj_exist(self, bucket, fnm, *args, **kwargs):
        try:
            if self.conn.head_object(Bucket=bucket, Key=fnm):
                return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            else:
                # For other errors (like 403 Forbidden), try GCS fallback
                logging.exception(f"S3 API obj_exist failed for {bucket}/{fnm}: {e}")

                if self.gcs_client:
                    try:
                        logging.info(
                            f"Trying GCS native client to check object {bucket}/{fnm}"
                        )
                        gcs_bucket = self.gcs_client.bucket(bucket)
                        blob = gcs_bucket.blob(fnm)
                        exists = blob.exists()
                        logging.info(
                            f"Object {bucket}/{fnm} exists: {exists} (via GCS native client)"
                        )
                        return exists
                    except Exception as gcs_e:
                        logging.exception(
                            f"GCS native obj_exist also failed for {bucket}/{fnm}: {gcs_e}"
                        )
                        return False

                # If no GCS fallback, assume file doesn't exist for non-404 errors
                return False

    @use_prefix_path
    @use_default_bucket
    def get_presigned_url(self, bucket, fnm, expires, *args, **kwargs):
        # For GCS endpoints, prioritize native GCS signed URLs with impersonation
        is_gcs = self.endpoint_url and "storage.googleapis.com" in self.endpoint_url

        if is_gcs and self.gcs_client:
            try:
                return self._generate_gcs_signed_url(bucket, fnm, expires, method="GET")
            except Exception as gcs_e:
                logging.warning(
                    f"GCS signed URL generation failed for {bucket}/{fnm}: {gcs_e}"
                )
                # Fall back to S3-compatible API

        # Use S3-compatible API for non-GCS or as fallback
        for _ in range(10):
            try:
                r = self.conn.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": bucket, "Key": fnm},
                    ExpiresIn=expires,
                )
                return r
            except Exception:
                logging.exception(f"fail get url {bucket}/{fnm}")
                self.__open__()
                time.sleep(1)
        return None

    @use_default_bucket
    def rm_bucket(self, bucket, *args, **kwargs):
        try:
            if not self.bucket_exists(bucket):
                return
            for o in self.conn.list_objects_v2(Bucket=bucket).get("Contents", []):
                self.conn.delete_object(Bucket=bucket, Key=o["Key"])
            self.conn.delete_bucket(Bucket=bucket)
        except Exception as e:
            logging.error(f"Fail rm {bucket}: " + str(e))

    def _get_content_type(self, filename):
        """Determine content type based on file extension"""
        content_type, _ = mimetypes.guess_type(filename)
        if content_type is None:
            # Default to binary if we can't determine the type
            content_type = "application/octet-stream"
        return content_type

    @use_prefix_path
    @use_default_bucket
    def get_presigned_upload_url(self, bucket, fnm, expires, content_type=None):
        """Generate a presigned URL for uploading files"""
        # Determine content type if not provided
        if content_type is None:
            content_type = self._get_content_type(fnm)

        # For GCS endpoints, use native GCS signed URLs with impersonation
        is_gcs = self.endpoint_url and "storage.googleapis.com" in self.endpoint_url

        if is_gcs and self.gcs_client:
            try:
                return self._generate_gcs_signed_url(
                    bucket, fnm, expires, method="PUT", content_type=content_type
                )
            except Exception as gcs_e:
                logging.warning(
                    f"GCS upload signed URL generation failed for {bucket}/{fnm}: {gcs_e}"
                )
                # Fall back to S3-compatible API

        # Use S3-compatible API for non-GCS or as fallback
        try:
            conditions = []
            if content_type:
                conditions.append({"Content-Type": content_type})

            r = self.conn.generate_presigned_url(
                "put_object",
                Params={"Bucket": bucket, "Key": fnm, "ContentType": content_type},
                ExpiresIn=expires,
            )
            return r
        except Exception as e:
            logging.exception(
                f"Failed to generate S3 upload URL for {bucket}/{fnm}: {e}"
            )
            return None

    def _generate_gcs_signed_url(
        self, bucket, object_name, expires, method="GET", content_type=None
    ):
        """Generate a signed URL for GCS using service account impersonation for toteqa project"""
        try:
            from google.auth import impersonated_credentials
            from google.auth import default
            from google.cloud import storage
            import datetime

            # Get default credentials first
            source_credentials, _ = default(
                scopes=["https://www.googleapis.com/auth/cloud-platform"]
            )

            # Service account to impersonate for toteqa project
            target_service_account = "ragflow-storage@toteqa.iam.gserviceaccount.com"

            # Create impersonated credentials
            target_credentials = impersonated_credentials.Credentials(
                source_credentials=source_credentials,
                target_principal=target_service_account,
                target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
                lifetime=3600,
            )

            # Create storage client with impersonated credentials
            storage_client = storage.Client(
                credentials=target_credentials, project="toteqa"
            )

            # Get the bucket and blob
            gcs_bucket = storage_client.bucket(bucket)
            blob = gcs_bucket.blob(object_name)

            # Set content type if provided (for uploads)
            if content_type and method == "PUT":
                blob.content_type = content_type

            # Generate the signed URL
            expiration = datetime.datetime.utcnow() + datetime.timedelta(
                seconds=expires
            )

            signed_url = blob.generate_signed_url(
                version="v4",
                expiration=expiration,
                method=method,
                content_type=content_type if method == "PUT" else None,
                credentials=target_credentials,
            )

            logging.info(
                f"Generated GCS signed URL for {method} {bucket}/{object_name} with impersonation"
            )
            return signed_url

        except Exception as e:
            logging.exception(
                f"Failed to generate GCS signed URL with impersonation: {e}"
            )
            raise e

    def get_upload_strategy(self, bucket, fnm, expires=3600):
        """
        Get the best upload strategy for the given file.
        Returns either a signed URL for direct upload or None if server-side upload should be used.
        """
        is_gcs = self.endpoint_url and "storage.googleapis.com" in self.endpoint_url

        if is_gcs and self.gcs_client:
            try:
                # For GCS, prefer signed URL uploads to avoid authentication issues
                content_type = self._get_content_type(fnm)
                signed_url = self.get_presigned_upload_url(
                    bucket, fnm, expires, content_type
                )

                if signed_url:
                    return {
                        "method": "signed_url",
                        "url": signed_url,
                        "content_type": content_type,
                        "expires": expires,
                    }
            except Exception as e:
                logging.warning(
                    f"Failed to generate signed URL for {bucket}/{fnm}: {e}"
                )

        # Fall back to server-side upload
        return {
            "method": "server_upload",
            "url": None,
            "content_type": self._get_content_type(fnm),
            "expires": None,
        }
