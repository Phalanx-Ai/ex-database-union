'''
Template Component main class.

'''
import csv
import logging
import pycurl
from collections import defaultdict

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

# configuration variables
KEY_COLUMNS = 'columns'
KEY_DATABASE_NAME = 'database'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_COLUMNS, KEY_DATABASE_NAME]
REQUIRED_IMAGE_PARS = []


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

        table_structures = self._get_table_structures()
        for table_name in table_structures.keys():
            # @todo: set all rows to the same destination (output can be folder?)
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
