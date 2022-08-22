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

import importlib
import os

from hsml.client.exceptions import RestAPIError

from hsml import client, util, constants

from hsml.core import experiment_api, dataset_api, run_api

from hsml.engine import local_engine, hopsworks_engine


class ExperimentEngine:
    def __init__(self):
        self._experiment_api = experiment_api.ExperimentApi()
        self._run_api = run_api.RunApi()
        self._dataset_api = dataset_api.DatasetApi()

        pydoop_spec = importlib.util.find_spec("pydoop")
        if pydoop_spec is None:
            self._engine = local_engine.LocalEngine()
        else:
            self._engine = hopsworks_engine.HopsworksEngine()

    def _set_id(
        self, run_instance, experiment_instance
    ):
        current_highest_id = 0
        for item in self._dataset_api.list(experiment_instance.path, sort_by="NAME:desc")[
            "items"
        ]:
            _, file_name = os.path.split(item["attributes"]["path"])
            try:
                try:
                    if '_' not in file_name:
                        continue
                    current_id = int(file_name.split('_')[1])
                except ValueError:
                    continue
                if current_id > current_highest_id:
                    current_highest_id = current_id
            except RestAPIError:
                pass
            
        run_instance.ml_id = experiment_instance.name + "_" + str("run_" + str(current_highest_id + 1))

        return run_instance

    def create(self, experiment):

        _client = client.get_instance()

        if not self._dataset_api.path_exists(constants.EXPERIMENTS_REGISTRY.EXPERIMENTS_DATASET):
            raise AssertionError(
                "{} dataset does not exist in this project. Please create it manually.".format(
                    constants.EXPERIMENTS_REGISTRY.EXPERIMENTS_DATASET
                )
            )

        self._engine.mkdir("/Projects/{}/{}/{}".format(_client._project_name, constants.EXPERIMENTS_REGISTRY.EXPERIMENTS_DATASET, experiment.name))

        experiment = self._experiment_api.put(
            experiment
        )

        print("Experiment created, explore it at " + experiment.get_url())

        return experiment

    def start_run(self, run_instance, experiment_instance):

        _client = client.get_instance()

        dataset_experiments_root_path = constants.EXPERIMENTS_REGISTRY.EXPERIMENTS_DATASET
        run_instance._project_name = _client._project_name
        run_instance._experiment_name = experiment_instance.name

        if not self._dataset_api.path_exists(dataset_experiments_root_path):
            raise AssertionError(
                "{} dataset does not exist in this project. Please create it manually.".format(
                    dataset_experiments_root_path
                )
            )

        dataset_experiment_name_path = dataset_experiments_root_path + "/" + experiment_instance.name
        if not self._dataset_api.path_exists(dataset_experiment_name_path):
            self._dataset_api.mkdir(dataset_experiment_name_path)

        run_instance = self._set_id(
            run_instance, experiment_instance
        )

        self._engine.mkdir(run_instance.path)

        run_configuration = {'type': 'runConfiguration',
                             'mlId': run_instance.ml_id,
                             'status': 'RUNNING'}

        run_instance = self._run_api.put(experiment_instance.name, run_configuration)

        print("Run created, explore it at " + run_instance.get_url())

        return run_instance

    def end_run(self, run_instance):

        _client = client.get_instance()

        run_instance._project_name = _client._project_name

        run_configuration = {'type': 'runConfiguration',
                             'mlId': run_instance.ml_id,
                             'status': 'FINISHED',
                             'parameters': run_instance._logged_params,
                             'metrics': run_instance._logged_metrics}

        self.upload_artifacts(run_instance)

        return self._run_api.put(run_instance._experiment_name, run_configuration)

    def upload_artifacts(self, run_instance):
        for artifact in run_instance.artifacts:
            if os.path.isfile(artifact.path):
                self._dataset_api.upload(artifact.path, run_instance.path)

    def save(self, experiment_instance):

        _client = client.get_instance()

        experiment = self._experiment_api.put(
            experiment_instance
        )

        return experiment