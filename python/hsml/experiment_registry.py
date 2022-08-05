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

import humps

from hsml.core import experiment_api
from hsml.tensorflow import signature as tensorflow_signature  # noqa: F401
from hsml.python import signature as python_signature  # noqa: F401
from hsml.sklearn import signature as sklearn_signature  # noqa: F401
from hsml.torch import signature as torch_signature  # noqa: F401

from hsml.experiment import Experiment

from hsml.client.exceptions import RestAPIError


class ExperimentRegistry:
    DEFAULT_VERSION = 1

    def __init__(
        self,
        project_name,
        project_id,
    ):
        self._project_name = project_name
        self._project_id = project_id

        self._experiment_api = experiment_api.ExperimentApi()

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        return cls(**json_decamelized)

    def get_experiment(self, name: str):
        """Get an experiment from the experiment registry.
        Getting an experiment from the Experiment Registry means getting its metadata handle, so you can subsequently register a new run.

        # Arguments
            name: Name of the experiment to get.
        # Returns
            `Experiment`: The experiment object.
        # Raises
            `RestAPIError`: If unable to retrieve experiment from the experiment registry.
        """

        return self._experiment_api.get(
            name,
        )

    def get_or_create_experiment(self, name: str):
        """Get an experiment from the experiment registry.
        Getting an experiment from the Experiment Registry means getting its metadata handle, so you can subsequently register a new run.

        # Arguments
            name: Name of the experiment to get.
        # Returns
            `Experiment`: The experiment object.
        # Raises
            `RestAPIError`: If unable to retrieve experiment from the experiment registry.
        """
        try:
            return self._experiment_api.get(
                name,
            )
        except RestAPIError as e:
            return Experiment(name, project_name=self._project_name)

    @property
    def project_name(self):
        """Name of the project the registry is connected to."""
        return self._project_name

    @property
    def project_path(self):
        """Path of the project the registry is connected to."""
        return "/Projects/{}".format(self._project_name)

    @property
    def project_id(self):
        """Id of the project the registry is connected to."""
        return self._project_id

    @property
    def experiment_registry_id(self):
        """Id of the experiment registry."""
        return self._experiment_registry_id

    def __repr__(self):
        return f"ExperimentRegistry(project: {self._project_name!r})"
