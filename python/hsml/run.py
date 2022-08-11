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
        run_folder=None,
        experiment_name=None,
        environment=None,
        program=None,
    ):
        self._id = id
        self._run_folder = run_folder
        self._experiment_name = experiment_name
        self._environment = environment
        self._program = program

        self._experiment_engine = experiment_engine.ExperimentEngine()

    def _start_run(self, experiment):
        return self._experiment_engine.start_run(self, experiment.path)

    def end_run(self):
        """Persist this model including model files and metadata to the model registry."""
        self._experiment_engine.end_run(self)

    @classmethod
    def from_response_json(cls, json_dict):
        json_decamelized = humps.decamelize(json_dict)
        if "count" in json_decamelized:
            if json_decamelized["count"] == 0:
                return []
            return [util.set_model_class(model) for model in json_decamelized["items"]]
        else:
            return util.set_model_class(json_decamelized)

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
            "runFolder": self._run_folder,
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
    def run_folder(self):
        """run_folder of the model."""
        return self._run_folder

    @run_folder.setter
    def run_folder(self, run_folder):
        self._run_folder = run_folder

    @property
    def experiment_name(self):
        """experiment_name of the model."""
        return self._experiment_name

    @experiment_name.setter
    def experiment_name(self, experiment_name):
        self._experiment_name = experiment_name

    @property
    def project_name(self):
        """project_name of the model."""
        return self._project_name

    @project_name.setter
    def project_name(self, project_name):
        self._project_name = project_name

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
    def path(self):
        """path of the model with version folder omitted. Resolves to /Projects/{project_name}/Models/{name}"""
        return "/Projects/{}/Experiments/{}/{}".format(self.project_name, self.experiment_name, str(self.run_folder))

    def __repr__(self):
        return f"Run(experiment: {self.experiment_name!r}, folder: {self.run_folder!r})"

    def get_url(self):
        path = (
            "/p/"
            + str(client.get_instance()._project_id)
            + "/experiments/"
            + str(self.experiment_name)
            + "/runs"
        )
        return util.get_hostname_replaced_url(path)
