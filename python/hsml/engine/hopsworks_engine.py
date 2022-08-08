#
#   Copyright 2021 Logical Clocks AB
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
#

from hsml.core import native_hdfs_api
from hsml import constants

from hsml.run import Run


class HopsworksEngine:
    def __init__(self):
        self._native_hdfs_api = native_hdfs_api.NativeHdfsApi()

    def mkdir(self, obj):
        if type(obj) == Run:
            self._native_hdfs_api.mkdir(obj.path)
            self._native_hdfs_api.chmod(obj.path, "ug+rwx")
        else:
            model_version_dir_hdfs = "/Projects/{}/{}/{}/{}".format(
                obj.project_name,
                constants.MODEL_SERVING.MODELS_DATASET,
                obj.name,
                str(obj.version),
            )
            self._native_hdfs_api.mkdir(model_version_dir_hdfs)
            self._native_hdfs_api.chmod(model_version_dir_hdfs, "ug+rwx")

    def delete(self, obj):
        model_version_dir_hdfs = "/Projects/{}/{}/{}/{}".format(
            obj.project_name,
            constants.MODEL_SERVING.MODELS_DATASET,
            obj.name,
            str(obj.version),
        )
        self._native_hdfs_api.delete(model_version_dir_hdfs)
