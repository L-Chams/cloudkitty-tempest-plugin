# Copyright 2017 Objectif Libre
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import logging

from tempest import config
from tempest.lib import exceptions

import time

from cloudkitty_tempest_plugin.tests.api import base


CONF = config.CONF
LOG = logging.getLogger(__name__)


class DataFrameCollectionScenarioTest(base.BaseRatingTest):

    """A scenario test class to test DataFrame collection.

    Steps:

    1. Create a volume resource
    2. Create a hashmap service for storage
    3. Create a field mapping for the volume resource
    4. Add details to the mapping (cost, flat rate, etc.)
    5. Wait for a specified amount of time
    6. Collect the dataframe and verify its correctness
    """

    api_version = 'v1'
    credentials = ['admin']

    @classmethod
    def setup_clients(cls):
        super(DataFrameCollectionScenarioTest, cls).setup_clients()
        # Add v2 client for scope state operations
        os_var = 'os_{}'.format(cls.credentials[0])
        cls.rating_client_v2 = getattr(cls, os_var).rating_clients['v2']

    def test_collect_dataframe(self):
        """Test DataFrame collection for volume resource."""
        self._setup_volume_resource()
        self._get_dataframe()
        self._check_dataframe()

    def _setup_volume_resource(self):
        # Create a volume resource
        self.volume = self.os_admin.vol_client.create_volume(
            size=2, name='cloudkitty_test_vol')
        self.volume_id = self.volume['volume']['id']
        self.project_id = self.os_admin.credentials.project_id
        self.user_id = self.os_admin.credentials.user_id
        self.addCleanup(self.os_admin.vol_client.delete_volume, self.volume_id)

        # Create a hashmap service for storage
        self.storage_service = self.rating_client.create_hashmap_service(
            name='storage')
        self.storage_service_id = self.storage_service['service_id']
        self.addCleanup(self.rating_client.delete_hashmap_service,
                        self.storage_service_id)

        # Enable hashmap module
        self.rating_client.update_rating_module(module_name='hashmap',
                                                enabled=True, priority=100)

        # Create a mapping
        self.mapping = self.rating_client.create_hashmap_mapping(
            cost=2, service_id=self.storage_service_id)
        self.mapping_id = self.mapping['mapping_id']
        self.addCleanup(self.rating_client.delete_hashmap_mapping,
                        self.mapping_id)

        # Wait until volume is accessible
        i = 0
        while i < 60:
            try:
                scope_state = self.rating_client_v2.get_scope_state(
                    scope_id=self.project_id)
                # Check if any result in the list is active for this project
                active = any(
                    result.get('active', False)
                    for result in scope_state.get('results', [])
                    if result.get('scope_id') == self.project_id
                )
                if active:
                    LOG.info(f'Scope {self.project_id} is active')
                    break
                else:
                    LOG.info(f'Scope {self.project_id} is not active yet.')
                    time.sleep(10)
                    i += 1
            except exceptions.NotFound:
                LOG.info(f'Scope {self.project_id} not found yet')
                time.sleep(10)
                i += 1

        # Reset scope to latest time
        updated_timestamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
        self.rating_client_v2.reset_scope_state(updated_timestamp,
                                                scope_id=self.project_id)
        LOG.info(f'Reset scope state with timestamp: {updated_timestamp}')

        LOG.info(f'Deactivating scope {self.project_id}')
        self.rating_client_v2.update_scope(scope_id=self.project_id,
                                           active=False)
        LOG.info(f'Deactivated scope {self.project_id}')

        time.sleep(60)

        self.rating_client_v2.update_scope(scope_id=self.project_id,
                                           active=True)
        LOG.info(f'Reactivated scope {self.project_id}')

        # wait to allow dataframe to be generated
        # testing 6.5 minutes (just over 2 collection cycles) to ensure data is
        # collected
        time.sleep(390)
        LOG.info('Finished waiting for dataframe generation')

    # Collect the dataframe and verify its correctness
    def _get_dataframe(self):

        self.dataframes = self.rating_client.get_storage_dataframes()

    def _check_dataframe(self):

        # Access the actual dataframes list
        is_dict = isinstance(self.dataframes, dict)
        if is_dict and 'dataframes' in self.dataframes:
            dataframes_list = self.dataframes['dataframes']
        else:
            if isinstance(self.dataframes, list):
                dataframes_list = self.dataframes
            else:
                dataframes_list = [self.dataframes]

        # Add validation before accessing
        if not dataframes_list:
            LOG.error(f"No dataframes collected. Response: {self.dataframes}")
            self.fail(
                f"No dataframes were collected. Received: {self.dataframes}")

        test_df = None
        for df in dataframes_list:
            curr_df = df['resources'][0]
            # check project id
            if 'project_id' in curr_df['desc']:
                project_id_field = 'project_id'
            else:
                project_id_field = 'tenant_id'
            if curr_df['desc'][project_id_field] == self.project_id:
                test_df = curr_df
                break

        if test_df is None:
            LOG.error(
                f"No matching dataframe found for volume with project ID "
                f"{self.project_id}. Dataframes: {dataframes_list}")
            self.fail(
                f"No matching dataframe found for volume with project ID "
                f"{self.project_id}.")

        self.assertEqual(test_df['service'], 'storage')
        self.assertEqual(test_df['desc']['id'], self.volume_id)
        # Check for either 'project_id' or 'tenant_id' as this varies
        if 'project_id' in test_df['desc']:
            project_id_field = 'project_id'
        else:
            project_id_field = 'tenant_id'
        self.assertEqual(test_df['desc'][project_id_field], self.project_id)
        self.assertEqual(test_df['desc']['user_id'], self.user_id)

        # check rating has a non-zero value
        self.assertGreater(float(test_df['rating']), 0.0)
