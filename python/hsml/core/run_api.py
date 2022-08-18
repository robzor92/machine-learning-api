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

from hsml import client, experiment, tag, run
import json
from typing import Union


class RunApi:
    def __init__(self):
        pass

    def put(self, experiment_name, run_configuration):
        """Save experiment run to the experiment registry.

        :param run_instance: metadata object of experiment run to be saved
        :type run_instance: Run
        :return: updated metadata object of the experiment
        :rtype: Experiment
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "experiments",
            experiment_name,
            "runs",
        ]

        print(run_configuration)

        headers = {"content-type": "application/json"}
        return run.Run.from_response_json(
            _client._send_request(
                "PUT",
                path_params,
                headers=headers,
                query_params={},
                data=json.dumps(run_configuration),
            ), _client._project_name, experiment_name
        )

    def delete(self, model_instance):
        """Delete the model and metadata.

        :param model_instance: metadata object of model to delete
        :type model_instance: Model
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "modelregistries",
            str(model_instance.model_registry_id),
            "models",
            model_instance.id,
        ]
        _client._send_request("DELETE", path_params)

    def set_tag(self, model_instance, name, value: Union[str, dict]):
        """Attach a name/value tag to a model.

        A tag consists of a name/value pair. Tag names are unique identifiers.
        The value of a tag can be any valid json - primitives, arrays or json objects.

        :param model_instance: model instance to attach tag
        :type model_instance: Model
        :param name: name of the tag to be added
        :type name: str
        :param value: value of the tag to be added
        :type value: str or dict
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "modelregistries",
            str(model_instance.model_registry_id),
            "models",
            model_instance.id,
            "tags",
            name,
        ]
        headers = {"content-type": "application/json"}
        json_value = json.dumps(value)
        _client._send_request("PUT", path_params, headers=headers, data=json_value)

    def delete_tag(self, model_instance, name):
        """Delete a tag.

        Tag names are unique identifiers.

        :param model_instance: model instance to delete tag from
        :type model_instance: Model
        :param name: name of the tag to be removed
        :type name: str
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "modelregistries",
            str(model_instance.model_registry_id),
            "models",
            model_instance.id,
            "tags",
            name,
        ]
        _client._send_request("DELETE", path_params)

    def get_tags(self, model_instance, name: str = None):
        """Get the tags.

        Gets all tags if no tag name is specified.

        :param model_instance: model instance to get the tags from
        :type model_instance: Model
        :param name: tag name
        :type name: str
        :return: dict of tag name/values
        :rtype: dict
        """
        _client = client.get_instance()
        path_params = [
            "project",
            _client._project_id,
            "modelregistries",
            str(model_instance.model_registry_id),
            "models",
            model_instance.id,
            "tags",
        ]

        if name is not None:
            path_params.append(name)

        return {
            tag._name: json.loads(tag._value)
            for tag in tag.Tag.from_response_json(
                _client._send_request("GET", path_params)
            )
        }
