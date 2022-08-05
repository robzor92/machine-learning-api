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

from hsml import client
from hsml.experiment_registry import ExperimentRegistry
from hsml.core import dataset_api
from hsml.client.exceptions import ExperimentRegistryException


class ExperimentRegistryApi:
    def __init__(self):
        self._dataset_api = dataset_api.DatasetApi()

    def get(self):
        """Get experiment registry.
        :return: the experiment registry metadata
        :rtype: ExperimentRegistry
        """
        _client = client.get_instance()

        # In the case of default model registry, validate that there is a Models dataset in the connected project
        if  not self._dataset_api.path_exists("Experiment"):
            raise ExperimentRegistryException(
                "No Experiments dataset exists in project {}, Please create the dataset manually.".format(
                    _client._project_name
                )
            )

        return ExperimentRegistry(
            _client._project_name,
            _client._project_id,
        )
