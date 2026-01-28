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
    credentials = ['admin']

    def test_collect_dataframe(self):
        """Test DataFrame collection for volume resource."""
        self._setup_volume_resource()
        self._get_dataframe()
        self._check_dataframe()


    def _setup_volume_resource(self):

        # create a volume resource
        self.volume = self.os_admin.vol_client.create_volume(size=2, name='cloudkitty_test_vol')
        self.volume_id = self.volume['volume']['id']
        self.project_id = self.os_admin.credentials.project_id
        self.addCleanup(self.os_admin.vol_client.delete_volume, self.volume_id)
        # create a hashmap service for storage
        self.storage_service = self.rating_client.create_hashmap_service(name='storage')
        self.storage_service_id = self.storage_service['service_id']
        self.addCleanup(self.rating_client.delete_hashmap_service, self.storage_service_id)
        # create a mapping
        # set to today's current date and time
        #start_date = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())


        # enable hashmap module
        self.rating_client.update_rating_module(module_name='hashmap', enabled=True,  priority=100)
        self.mapping = self.rating_client.create_hashmap_mapping(cost=2,
                                                            service_id=self.storage_service_id)
        self.mapping_id = self.mapping['mapping_id']
        self.addCleanup(self.rating_client.delete_hashmap_mapping, self.mapping_id)

        # wait for a specified amount of time
        time.sleep(600) #testing 10 minutes

    # collect the dataframe and verify its correctness
    def _get_dataframe(self):

       self.dataframes = self.rating_client.get_storage_dataframes()



    def _check_dataframe(self):
         # Access the actual dataframes list
        if isinstance(self.dataframes, dict) and 'dataframes' in self.dataframes:
            dataframes_list = self.dataframes['dataframes']
        else:
            dataframes_list = self.dataframes if isinstance(self.dataframes, list) else [self.dataframes]

        # Add validation before accessing
        if not dataframes_list:
            LOG.error(f"No dataframes collected. Response: {self.dataframes}")
            self.fail(f"No dataframes were collected. Received: {self.dataframes}")


        # loop through dataframes and look for the one that matches self.volume id

        for df in dataframes_list:
            if df['desc']['id'] == self.volume['volume']['id']:
                test_df = df
                break
        else:
            LOG.error(f"No matching dataframe found for volume ID {self.volume['volume']['id']}. Dataframes: {dataframes_list}")
            self.fail(f"No matching dataframe found for volume ID {self.volume['volume']['id']}.")


        #last_df = dataframes_list[-1]['resources'][0]

        with open('/home/ubuntu/dataframe_info_7.txt', 'w') as f:
            #first print desired outputs
            print("self.volume id:", self.volume['volume']['id'], file=f)
            print("self.volume project_id:", self.project_id, file=f)
            print("self.volume user_id:", self.volume['volume']['user_id'], file=f)

            for df in dataframes_list:
                print("DataFrame:", df, file=f)
                for resource in df.get('resources', []):
                    print("Resource:", resource, file=f)




        self.assertEqual(test_df['service'], 'storage') #works
        self.assertEqual(test_df['desc']['id'], self.volume['volume']['id'])
        self.assertEqual(test_df['desc']['project_id'], self.project_id)
        self.assertEqual(test_df['desc']['user_id'], self.volume['volume']['user_id'])

        #check rating has a non-zero value
        self.assertGreater(float(test_df['rating']), 0.0)