

import logging
import time
from tempest import config
from tempest.lib import decorators
from tempest.lib import exceptions
from tempest.lib.common.utils import data_utils
from tempest.lib.services.volume.v3 import volumes_client

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

    Need to combine with low [collect] period and wait_period, and match the
    Prometheus scrape interval to ensure data is collected.
    """

    """ This is what the usual CLI commands are to set up:

    openstack rating hashmap service create storage # create a hashmap service for storage
    STORAGE_SERVICE_ID=$(openstack rating hashmap service list -f value | grep storage | awk '{ print $2 }') # get the ID of the created service
    openstack rating hashmap mapping create -s $STORAGE_SERVICE_ID -t flat 2 # create a field mapping for the volume resource with a flat rate of 2
    openstack volume create --size 2 vol1 # create a volume resource
"""
    api_version = 'v1'
    credentials = ['admin'] #idk if I need this


    def test_collect_dataframe(self):
        """Test DataFrame collection for volume resource."""

        self._setup_volume_resource()
        self._get_dataframe()
        self._check_dataframe()


    def _setup_volume_resource(self):

        with open('/home/ubuntu/out.txt', 'a') as f:
            print("client_manager is", self.client_manager, file=f)
        # create a volume resource
        self.volume = self.client_manager.vol_client.create_volume(size=2, name='cloudkitty_test_vol')


        # create a hashmap service for storage
        storage_service = self.rating_client.create_hashmap_field(self, storage)
        storage_service_id = storage_service['id']

        mapping = self.rating_client.create_hashmap_mapping(self, cost=2, field_id=None, group_id=None,
                                    map_type='flat', mapping_id=None, service_id=storage_service_id,
                                    tenant_id=None, value=None)

        # wait for a specified amount of time
        time.sleep(120) #????

    # collect the dataframe and verify its correctness
    def _get_dataframe(self):

        dataframes = self.client_manager.get_storage_dataframes()
        return dataframes



    def _check_dataframe(self):

        dataframes = self.get_dataframe()

        expected_dataframe = {'rating': '0.8',
                            'service': 'storage',
                            'desc': {
                                'volume_type': 'lvmdriver-1',
                                'id': self.volume['id'],
                                'project_id': self.volume['project_id'],
                                'user_id': self.volume['user_id'],
                                'week_of_the_year':  today.week, #dont really care about checking datetime stamps  - can I ignore?
                                'day_of_the_year': today.day,
                                'month': today.month,
                                'year': today.year
                            },
                            'volume': '0.4',
                            'rate_value': '2.0000'}

        #check if service, id, project_id and user_id match
        self.assertEqual(dataframes[0]['service'], expected_dataframe['service'])
        self.assertEqual(dataframes[0]['desc']['id'], expected_dataframe['desc']['id'])
        self.assertEqual(dataframes[0]['desc']['project_id'], expected_dataframe['desc']['project_id'])
        self.assertEqual(dataframes[0]['desc']['user_id'], expected_dataframe['desc']['user_id'])

        #check rating has a non-zero value
        self.assertGreater(float(dataframes[0]['rating']), 0.0)

        #clean everything up