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

from hsml.core import experiment_api, dataset_api

from hsml.engine import local_engine, hopsworks_engine


class ExperimentEngine:
    def __init__(self):
        self._experiment_api = experiment_api.ExperimentApi()
        self._dataset_api = dataset_api.DatasetApi()

        pydoop_spec = importlib.util.find_spec("pydoop")
        if pydoop_spec is None:
            self._engine = local_engine.LocalEngine()
        else:
            self._engine = hopsworks_engine.HopsworksEngine()

    def _set_id(
        self, run_instance, dataset_experiment_path
    ):
        # Set run id if not defined
        if run_instance.run_folder is None:
            current_highest_id = 0
            for item in self._dataset_api.list(dataset_experiment_path, sort_by="NAME:desc")[
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
            run_instance.run_folder = "run_" + str(current_highest_id + 1)
            
        run_instance.id = run_instance.experiment_name + "_" + str(run_instance.run_folder)

        return run_instance

    def start_run(self, run_instance, experiment_path):

        _client = client.get_instance()

        dataset_experiments_root_path = constants.EXPERIMENTS_REGISTRY.EXPERIMENTS_DATASET
        run_instance._project_name = _client._project_name

        if not self._dataset_api.path_exists(dataset_experiments_root_path):
            raise AssertionError(
                "{} dataset does not exist in this project. Please create it manually.".format(
                    dataset_experiments_root_path
                )
            )

        dataset_experiment_name_path = dataset_experiments_root_path + "/" + run_instance.experiment_name
        if not self._dataset_api.path_exists(dataset_experiment_name_path):
            self._dataset_api.mkdir(dataset_experiment_name_path)

        run_instance = self._set_id(
            run_instance, experiment_path
        )

        self._engine.mkdir(run_instance)

        run_query_params = {}

        run_instance = self._experiment_api.put(
            run_instance, run_query_params
        )

        print("Run created, explore it at " + run_instance.get_url())

        return run_instance