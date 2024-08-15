'''
Template Component main class.

'''
import csv
import logging
import pycurl
import json
from io import BytesIO
from collections import defaultdict

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
KEY_COLUMNS = 'columns'
KEY_DATABASE_NAME = 'database'
KEY_AUTH_TOKEN = '#api_token'
KEY_EXAMPLE_CONFIGURATION_ID = "example_config_id"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_COLUMNS, KEY_DATABASE_NAME, KEY_EXAMPLE_CONFIGURATION_ID,
                       KEY_AUTH_TOKEN]
REQUIRED_IMAGE_PARS = []

# @note: This is fixed as change of component requires code modification
ROW_COMPONENT_ID = "keboola.ex-ftp"


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()

    def _get_table_structures(self):
        KEY_TABLE_NAME = "table"
        KEY_COLUMN_NAME = "column"

        KEY_COMPANY_NAME = "_company"
        KEY_COMPANY_YEAR = "_year"

        tables = defaultdict(lambda: list([KEY_COMPANY_NAME, KEY_COMPANY_YEAR]))
        for column in self.configuration.parameters[KEY_COLUMNS]:
            tables[column[KEY_TABLE_NAME]].append(column[KEY_COLUMN_NAME])

        return tables

    def _get_unique_prefix_for_db(self):
        # @todo: fix according to real databases name
        name_parts = self.configuration.parameters[KEY_DATABASE_NAME].split('_', 3)

        DATABASE_COMPANY = name_parts[0]
        DATABASE_YEAR = name_parts[1]

        return "%s_%s_" % (DATABASE_COMPANY, DATABASE_YEAR)

    def run(self):
        # check for missing configuration parameters
        self.validate_configuration_parameters(REQUIRED_PARAMETERS)
        self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        params = self.configuration.parameters
        logging.info(params)

        logging.info("Keboola API part - get example configuration")
        token = params.get(KEY_AUTH_TOKEN)
        configuration_id = params.get(KEY_EXAMPLE_CONFIGURATION_ID)
        URL_GET_CONFIG_DETAIL = \
            "https://connection.north-europe.azure.keboola.com/v2/storage/components/%s/configs/%s" % \
            (ROW_COMPONENT_ID, configuration_id)

        http_data = BytesIO()
        curl = pycurl.Curl()
        # @todo: development only - token from Chrome works; token from Keboola not
        curl.setopt(pycurl.SSL_VERIFYHOST, 0)
        curl.setopt(pycurl.SSL_VERIFYPEER, 0)
        #
        curl.setopt(pycurl.URL, URL_GET_CONFIG_DETAIL)
        curl.setopt(pycurl.HTTPHEADER, ["X-StorageApi-Token: %s" % (token)])
        curl.setopt(pycurl.WRITEFUNCTION, http_data.write)
        curl.perform()

        example_configuration = json.loads(http_data.getvalue())
        print(example_configuration)

        logging.info("CSV tables part")
        table_structures = self._get_table_structures()
        for table_name in table_structures.keys():
            csv_filename = self._get_unique_prefix_for_db() + "%s.csv" % (table_name)

            table = self.create_out_table_definition(
                csv_filename,
                incremental=False,
                columns=table_structures[table_name])
            self.write_manifest(table)

            with open(table.full_path, mode='wt', encoding='utf-8', newline='') as out_file:
                writer = csv.DictWriter(
                    out_file,
                    fieldnames=table_structures[table_name]
                )
                writer.writeheader()

                # @todo: REMOVE (insert debug data only)
                if len(writer.fieldnames) == 4:
                    writer.writerow({
                       '_company': 'Phalanx',
                       '_year': '2023',
                       'id': 1,
                       'suma': 1000
                    })
                    writer.writerow({
                       '_company': 'Phalanx',
                       '_year': '2023',
                       'id': 2,
                       'suma': 555
                    })
                elif len(writer.fieldnames) == 3:
                    writer.writerow({
                       '_company': 'Phalanx',
                       '_year': '2023',
                       'id': 141,
                    })
                    writer.writerow({
                       '_company': 'Phalanx',
                       '_year': '2023',
                       'id': 333,
                    })


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
