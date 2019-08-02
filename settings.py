import platform
import configparser
import re
import json
import os
import datetime
import fb_run_eddm

"""
Home to the GlobalVar class, and other classes and functions that 
need to be accessible through the whole application 
"""


class GlobalVar:
    def __init__(self):
        config = configparser.ConfigParser()
        config.read(os.path.join(os.curdir, 'config.ini'))

        self.order_db = os.path.join(os.curdir, 'orders.db')

        self.fb_token = config['token']['fb_token']
        # self.fb_qa_token = config['token']['fbmtk-qa_token']
        # Changed this token to the production token, now working with live orders 2019-07-22
        self.fb_qa_token = config['token']['fb_token']
        self.closeout_token = config['token']['closeout_token']

        # Push XML to MarcomCentral
        self.closeout_url = config['closeout']['production']
        self.closeout_url_wsdl = config['closeout']['production_wsdl']

        # Pull XML order information from MarcomCentral platform - each portal will have a different Token
        self.order_url = config['order']['production']
        self.order_url_wsdl = config['order']['production_wsdl']

        self.jobticket_url = config['jobticket']['production']
        self.jobticket_url_wsdl = config['jobticket']['production_wsdl']

        self.mssql_connection = config['mssql']['connection']
        self.mssql_database = config['mssql']['database']
        self.mssql_user = config['mssql']['user']
        self.mssql_pass = config['mssql']['pass']

        # VERY IMPORTANT
        self.environment = ''
        self.token = ''
        self.db_name = ''

        self.token_names = {'PRODUCTION': 'Farm Bureau',
                            'QA': 'Farm Bureau QA',
                            }

        self.db_names = {'PRODUCTION': 'eddm_db.db',
                         'QA': 'eddm_db_qa.db'
                         }

        # Environment Specific
        if platform.system() != 'Linux':
            self.user_data_path = (os.path.join('\\\\JTSRV4', 'Data', 'Customer Files', 'In Progress',
                                                '01-Web Storefront DBs', 'FB Marketing Toolkit', 'Current',
                                                'V2FBLUSERDATA.TXT'))
            self.web_to_print_path = os.path.join('\\\\JTSRV3','Print Facility', 
                                                  'Job Ticket Feed docs','WebToPrint')
            self.ftp_directory = os.path.join('\\\\JTSRV3','Print Facility','FTPfiles','LocalUser','FB-EDDM')
        else:
            self.user_data_path = (os.path.join(os.path.abspath(os.sep), 'media', 'Network', 
                                                'Customer Files', 'In Progress',
                                                '01-Web Storefront DBs', 'FB Marketing Toolkit', 'Current',
                                                'V2FBLUSERDATA.TXT'))

            self.web_to_print_path = (os.path.join(os.path.abspath(os.sep), 'media','Print', 
                                                   'Job Ticket Feed docs','WebToPrint'))
            self.ftp_directory = (os.path.join(os.path.abspath(os.sep), 'media','Print',
                                              'FTPfiles','LocalUser','FB-EDDM'))

        self.downloaded_orders_path = os.curdir
        self.save_orders_path = os.curdir
        self.accuzip_path = os.curdir
        self.reset_routes_path = os.curdir
        self.duplicate_orders_path = os.curdir
        self.hold_orders_path = os.curdir
        self.no_match_orders_path = os.curdir
        self.complete_processing_path = os.curdir
        self.deleted_orders_path = os.curdir

        self.log_messages = []
        self.delete_original_files = True

    def create_accuzip_dir(self):
        if not os.path.exists(self.accuzip_path):
            os.mkdir(self.accuzip_path)

    def set_order_paths(self):
        if self.environment.upper() == 'PRODUCTION':
            self.downloaded_orders_path = os.path.join(os.path.join(os.curdir, 'unprocessed_orders'))
            self.accuzip_path = os.path.join(os.curdir, 'accuzip_orders')
            self.reset_routes_path = os.path.join(os.curdir, 'reset_routes')
            self.save_orders_path = os.path.join(os.curdir, 'success_orders')
            self.hold_orders_path = os.path.join(os.curdir, 'hold_orders')
            self.duplicate_orders_path = os.path.join(self.hold_orders_path, 'duplicate_orders')
            self.no_match_orders_path = os.path.join(self.hold_orders_path, 'no_order_match')
            self.complete_processing_path = os.path.join(os.curdir, 'complete_processing_files')
            self.deleted_orders_path = os.path.join(os.curdir, 'deleted_orders')
        else:
            self.downloaded_orders_path = os.path.join(os.path.join(os.curdir, 'fb-eddm', 'test'))
            self.accuzip_path = os.path.join(self.downloaded_orders_path, 'accuzip_orders')
            self.reset_routes_path = os.path.join(self.downloaded_orders_path, 'reset_routes')
            self.save_orders_path = os.path.join(self.downloaded_orders_path, 'success_orders')
            self.hold_orders_path = os.path.join(self.downloaded_orders_path, 'hold_orders')
            self.duplicate_orders_path = os.path.join(self.hold_orders_path, 'duplicate_orders')
            self.no_match_orders_path = os.path.join(self.hold_orders_path, 'no_order_match')
            self.complete_processing_path = os.path.join(self.downloaded_orders_path, 'complete_processing_files')
            self.deleted_orders_path = os.path.join(self.downloaded_orders_path, 'deleted_orders')

    def set_environment(self, env):
        # set to 'Production' for production, anything else, not production
        self.environment = env.upper()

    def set_token_name(self):
        if self.environment == 'PRODUCTION':
            self.token = self.fb_token
        else:
            self.token = self.fb_qa_token

    def set_db_name(self):
        self.db_name = self.db_names[self.environment]

    def print_log(self, message):
        """
        Sends message to console, saves message to message log to be recalled later
        :param message: text message to console
        """
        print(message)
        self.log_messages.append(message)


class EDDMOrder:
    def __init__(self):
        # Settings from the orginal GlobalVar()
        self.mail_residential = False
        self.file_touches = 0
        self.order_touches = 0
        self.file_qty = 0
        self.order_qty = 0
        self.touch_1_maildate = datetime.date.today()
        self.touch_2_maildate = datetime.date.today()
        self.jobname = ''
        self.processing_messages = {'count_match': True,
                                    'touch_match': True,
                                    'date_match': True}

        self.dat_header = ["AgentID", "DateSelected", "City", "State",
                           "ZipCode", "RouteID", "Quantity", "POS",
                           "NumberOfTouches"]

    def set_mailing_residential(self, val):
        self.mail_residential = bool(val)

    def record_addressee(self, route):
        if route[0] == 'B':
            return 'PO BOX HOLDER'
        elif self.mail_residential:
            return 'RESIDENTIAL CUSTOMER'
        return 'POSTAL CUSTOMER'

    def set_touch_2_maildate(self, file_date):
        """Touch 2 is 33 days from the order date"""
        proc_dt = datetime.datetime.strptime(file_date, "%Y%m%d%H%M%S")
        proc_dt = proc_dt + datetime.timedelta(days=33)
        day_of_week = proc_dt.isoweekday()

        while day_of_week > 5:
            proc_dt = proc_dt + datetime.timedelta(days=1)
            day_of_week = proc_dt.isoweekday()

        self.touch_2_maildate = proc_dt

    def set_touch_1_maildate(self, file_date):
        """Touch 1 is 5 days from the order date"""
        proc_dt = datetime.datetime.strptime(file_date, "%Y%m%d%H%M%S")
        proc_dt = proc_dt + datetime.timedelta(days=5)
        day_of_week = proc_dt.isoweekday()

        while day_of_week > 5:
            proc_dt = proc_dt + datetime.timedelta(days=1)
            day_of_week = proc_dt.isoweekday()

        self.touch_1_maildate = proc_dt


class NonMatchOrders:
    def __init__(self, hours):
        self.hour_threshold = hours
        self.file_under_threshold = []
        self.file_over_threshold = []

    def set_threshold_lists(self, gblv, to_console=False):
        # list all files in non-match path
        all_non_match = [f for f in os.listdir(gblv.no_match_orders_path) if f[-3:].upper() == 'DAT']
        if len(all_non_match) > 0 and to_console:
            gblv.print_log("Non-match records\n\t{:<45}{:<10}".format("File Name", "Hour"))
        # Run through all_non_match to make new list of those older than
        # self.hour_threshold hours and younger than self.hour_threshold hours
        for non in all_non_match:
            diff = datetime.datetime.utcnow() - datetime.datetime.strptime(non[-18:-4], "%Y%m%d%H%M%S")
            days, seconds = diff.days, diff.seconds
            hours = days * 24 + seconds // 3600

            if to_console:
                gblv.print_log("\t{:<45}{:<10}".format(non, hours))

            if hours <= self.hour_threshold:
                self.file_under_threshold.append(non)
            else:
                self.file_over_threshold.append(non)


def clean_json(json_string):
    """
    This is a stupid way to deal with a problem it was taking me all f-ing day
    to figure out.  You're welcome.

    :param json_string as string to clean up json formatting
    """
    json_string = "".join(str(json_string))

    rpl_1 = re.compile("': '")
    rpl_2 = re.compile("\s{4}'")
    rpl_3 = re.compile("':")
    rpl_4 = re.compile("',\n")
    rpl_5 = re.compile("'\n")
    rpl_6 = re.compile('=""')
    rpl_7 = re.compile("\\'")
    rpl_8 = re.compile('[\d]""')

    cleaned_json = re.sub(rpl_1, '": "', json_string)
    cleaned_json = re.sub(rpl_2, '    "', cleaned_json)
    cleaned_json = re.sub(rpl_3, '":', cleaned_json)
    cleaned_json = re.sub(rpl_4, '",\n', cleaned_json)
    cleaned_json = re.sub(rpl_5, '"\n', cleaned_json)
    cleaned_json = re.sub(rpl_6, '=\"\"', cleaned_json)

    cleaned_json = str.replace(cleaned_json, "\\'", "'")

    for r in rpl_8.findall(cleaned_json):
        cleaned_json = re.sub(r, '{}\\""'.format(r[:-2]), cleaned_json)

    return cleaned_json


def search_json_list(cursor, search_key, search_value, result_key, contains_keys=False):
    """
    Returns a list of matching values in a json array
    Takes connection cursor and key result returns dictionary (key, result)
    If contains_keys is True, looks to first field in cursor results for key
    If contains_keys is False, assigns dictionary key as position in cursor result

    :parameter cursor: object after cursor.execute()
    :parameter search_value: value to match in key search
    :parameter search_key: object key to search
    :parameter result_key: key to look in for value

    Example:
        {
        search_key : search_value,
        result_key : [FUNCTION RETURNS THIS VALUE]
        }
    """
    cursor_results = cursor.fetchall()
    key_list = []

    if contains_keys:
        key_list = [r[0] for r in cursor_results]
        cursor_results = ([r[1] for r in cursor_results])

    if not key_list:
        key_list = range(1, len(cursor_results) + 1)

    return_result = dict()

    if not contains_keys:
        for key, fetch in zip(key_list, cursor_results):
            for rec in fetch:
                template = json.loads(rec)
                result = ([field[result_key] for field in template
                           if field[search_key] == search_value])
                if result:
                    return_result[key] = result[0]
    else:
        for key, fetch in zip(key_list, cursor_results):
            print(key, fetch)
            template = json.loads(fetch)
            result = ([field[result_key] for field in template
                       if field[search_key] == search_value])
            if result:
                return_result[key] = result[0]

    return return_result


def main():
    pass


if __name__ == '__main__':
    main()
