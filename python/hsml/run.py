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
class Run:
    """Metadata object representing an experiment in the Experiment Registry."""

    def __init__(
        self,
        id=None,
        ml_id=None,
        started=None,
        finished=None,
        status=None,
        creator=None,
        environment=None,
        program=None,
        parameters=None,
        metrics=None,
        experiment_name=None,
        project_name=None,
    ):
        self._id = id
        self._ml_id = ml_id
        self._started = started
        self._finished = finished
        self._status = status
        self._creator = creator
        self._environment = environment
        self._program = program
        self._parameters = parameters
        self._metrics = metrics

        self._experiment_name = experiment_name
        self._project_name = project_name

        self._experiment_engine = experiment_engine.ExperimentEngine()

    def _start_run(self, experiment):
        return self._experiment_engine.start_run(self, experiment.path)

    def end_run(self):
        """Persist this model including model files and metadata to the model registry."""
        self._experiment_engine.end_run(self)

    @classmethod
    def from_response_json(cls, json_dict, project_name, experiment_name):
        json_decamelized = humps.decamelize(json_dict)
        if "count" not in json_decamelized:
            return cls(**json_decamelized, project_name=project_name, experiment_name=experiment_name)
        elif json_decamelized["count"] == 0:
            return []
        else:
            return [
                cls(**run, project_name=project_name, experiment_name=experiment_name)
                for run in json_decamelized["items"]
            ]

    def update_from_response_json(self, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        _ = json_decamelized.pop("type")
        self.__init__(**json_decamelized)
        return self

    def json(self):
        return json.dumps(self, cls=util.MLEncoder)

    def to_dict(self):
        run = {
            "id": self._id,
            "experimentName": self._experiment_name,
        }

        if hasattr(self, "environment"):
            run["environment"] = self._environment

        if hasattr(self, "program"):
            run["program"] = self._program

        return run

    @property
    def id(self):
        """id of the model."""
        return self._id

    @id.setter
    def id(self, id):
        self._id = id

    @property
    def ml_id(self):
        """id of the model."""
        return self._ml_id

    @ml_id.setter
    def ml_id(self, ml_id):
        self._ml_id = ml_id

    @property
    def started(self):
        """started of the model."""
        return self._started

    @started.setter
    def started(self, started):
        self._started = started

    @property
    def finished(self):
        """finished of the model."""
        return self._finished

    @finished.setter
    def finished(self, finished):
        self._finished = finished

    @property
    def status(self):
        """status of the model."""
        return self._status

    @status.setter
    def status(self, status):
        self._status = status

    @property
    def creator(self):
        """status of the model."""
        return self._creator

    @creator.setter
    def creator(self, creator):
        self._creator = creator

    @property
    def environment(self):
        """environment of the model."""
        return self._environment

    @environment.setter
    def environment(self, environment):
        self._environment = environment

    @property
    def program(self):
        """program of the model."""
        return self._program

    @program.setter
    def program(self, program):
        self._program = program

    @property
    def parameters(self):
        """parameters of the model."""
        return self._parameters

    @parameters.setter
    def parameters(self, parameters):
        self._parameters = parameters

    @property
    def metrics(self):
        """parameters of the model."""
        return self._metrics

    @parameters.setter
    def parameters(self, metrics):
        self._metrics = metrics

    @property
    def path(self):
        """path of the model with version folder omitted. Resolves to /Projects/{project_name}/Models/{name}"""
        return "/Projects/{}/Experiments/{}/{}".format(self._project_name, self._experiment_name, self.ml_id[(len(self._experiment_name)+1):])

    def __repr__(self):
        return f"Run(experiment: {self._experiment_name!r})"

    def get_url(self):
        path = (
            "/p/"
            + str(client.get_instance()._project_id)
            + "/experiments/"
            + str(self._experiment_name)
            + "/runs"
        )
        return util.get_hostname_replaced_url(path)
