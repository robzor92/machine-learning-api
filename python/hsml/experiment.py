#
#   Copyright 2022 Logical Clocks AB
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

import json
import humps

from hsml.engine import experiment_engine

from hsml import client, util

from hsml.run import Run
class Experiment:
    """Metadata object representing an experiment in the Experiment Registry."""

    def __init__(
        self,
        id=None,
        name=None,
        created=None,
        runs=None,
        description=None,
        creator=None,
        project_name=None,
        type=None,
        items=None,
        href=None,
    ):
        self._id = id

        self._name = name

        self._created = created

        self._runs = runs

        self._description = description

        self._creator = creator

        self._project_name = project_name

        self._experiment_engine = experiment_engine.ExperimentEngine()

    def start_run(self):
        """Start the run."""
        run = Run(experiment_name=self.name)
        run = run._start_run(self)
        return run

    def delete(self):
        """Delete the experiment and all recorded runs

        !!! danger "Potentially dangerous operation"
            This operation drops all runs associated with this experiment.

        # Raises
            `RestAPIError`.
        """
        self._experiment_engine.delete(self)

    @classmethod
    def from_response_json(cls, json_dict, project_name):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [cls(**model, project_name=project_name) for model in json_decamelized["items"]]
        else:
            return cls(**json_decamelized, project_name=project_name)

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.MLEncoder)

    def to_dict(self):
        return {

        }

    @property
    def id(self):
        """Id of the model."""
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def name(self):
        """Name of the model."""
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @property
    def created(self):
        """created of the model."""
        return self._created

    @created.setter
    def created(self, created):
        self._created = created

    @property
    def runs(self):
        """runs of the model."""
        return self._runs

    @runs.setter
    def runs(self, runs):
        self._runs = runs

    @property
    def description(self):
        """description date of the model."""
        return self._description

    @description.setter
    def description(self, description):
        self._description = description

    @property
    def creator(self):
        """creator example of the model."""
        return self._creator

    @creator.setter
    def creator(self, creator):
        self._creator = creator

    @property
    def path(self):
        """path of the experiment. Resolves to /Projects/{project_name}/Experiments/{name}"""
        return "/Projects/{}/Experiments/{}".format(self._project_name, self.name)

    def get_url(self):
        path = (
            "/p/"
            + str(client.get_instance()._project_id)
            + "/experiments"
        )
        return util.get_hostname_replaced_url(path)
