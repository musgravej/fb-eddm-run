import os
import pytz
import zeep
import datetime
import bs4
import sqlite3
import settings
import re
import pytds

"""
Error during get orders as: prepare_for_mysql result = self._cmysql.convert_to_mysql(*params)
    is probably from an order detail object not being formatted as a json object
    Look at error message: Python type ArrayOfOrderDetailType**FinishingOption** cannot be converted
    to show which object.
"""


def conntect_message():
    print("succesffully connected to get_order_by_date.py")


def find_template_value_drops(template_field):
        for elem in template_field:
            # print(elem)
            if elem['Name'] == 'Drops':
                return elem['Value'][-1]


class Supplier:
    def __init__(self):
        self.groups = []

    def append_to_group(self, od):
        self.groups.append({'id': od['Supplier']['ID']['_value_1'],
                            'name': od['Supplier']['Name']})


class OrderDetail:
    # create a order_detail list object for OrderDetail table
    def __init__(self):
        self.groups = []

    def append_to_group(self, od, rec):
        # Remove pesky non-ascii characters
        ascii_replace = re.compile(r'[^\x00-\x7F]+')

        # if not isinstance(od['TemplateFields'], type(None)):
        #     find_template_value(od['TemplateFields']['TemplateField'], 'Drops')

        self.groups.append({'order_id': rec,
                            'eddm_touches': (find_template_value_drops(od['TemplateFields']['TemplateField'])
                                             if od['TemplateFields'] is not None else None),
                            'order_detail_id': od['ID']['_value_1'],
                            'order_type': od['OrderType'],
                            'user_id': od['User']['ID']['_value_1'],
                            'req_user': od['ReqUser'],
                            'product_id': od['ProductID']['_value_1'],
                            'product_name': od['ProductName'],
                            'product_description':
                                (bs4.BeautifulSoup(od['ProductDescription'], features="lxml").get_text()[:100] if
                                 od['ProductDescription'] is not None else None),
                            'sku_id': od['SKU']['ID']['_value_1'],
                            'sku_name': od['SKU']['Name'],
                            'sku_description': (bs4.BeautifulSoup(od['SKUDescription'], features="lxml").get_text()
                                                if od['SKUDescription'] is not None else None),


                            'template_fields': (settings.clean_json(od['TemplateFields']['TemplateField']) if
                                                od['TemplateFields'] is not None else None),

                            'quantity': od['Quantity'],
                            'quantity_shipped': od['QuantityShipped'],
                            'price_customer': od['Price']['Cost']['Customer']['_value_1'],
                            'price_seller': od['Price']['Cost']['Seller']['_value_1'],
                            'price_shipping': od['Price']['Cost']['Shipping']['_value_1'],
                            'price_unit': od['Price']['Cost']['Unit']['_value_1'],
                            'price_customer_discount': od['Price']['Cost']['CustomerDiscount']['_value_1'],
                            'price_seller_misc': od['Price']['Cost']['SellerMisc']['_value_1'],
                            'price_seller_store_discount': od['Price']['Cost']['SellerStoreDiscount']['_value_1'],
                            'price_seller_shipping': od['Price']['Cost']['SellerShipping']['_value_1'],
                            'price_customer_store_discount': od['Price']['Cost']['CustomerStoreDiscount']['_value_1'],
                            'price_postage': od['Price']['Cost']['Postage']['_value_1'],
                            'price_customer_misc': od['Price']['Cost']['CustomerMisc']['_value_1'],
                            'tax_customer_sales': od['Price']['Tax']['CustomerSales']['_value_1'],
                            'tax_direct_acct_sales': od['Price']['Tax']['DirectAcctSales']['_value_1'],
                            'tax_city': od['Price']['Tax']['City']['_value_1'],
                            'tax_county': od['Price']['Tax']['County']['_value_1'],
                            'tax_state': od['Price']['Tax']['State']['_value_1'],
                            'tax_district': od['Price']['Tax']['District']['_value_1'],
                            'tax_city_freight': od['Price']['Tax']['CityFreight']['_value_1'],
                            'tax_county_freight': od['Price']['Tax']['CountyFreight']['_value_1'],
                            'tax_state_freight': od['Price']['Tax']['StateFreight']['_value_1'],
                            'tax_district_freight': od['Price']['Tax']['DistrictFreight']['_value_1'],
                            'tax_total_freight': od['Price']['Tax']['TotalFreight']['_value_1'],
                            'tax_taxable_sales': od['Price']['Tax']['TaxableSalesAmount']['_value_1'],
                            'tax_exempt_sales': od['Price']['Tax']['ExemptSalesAmount']['_value_1'],
                            'tax_non_taxable': od['Price']['Tax']['NonTaxableSalesAmount']['_value_1'],
                            'tax_city_name': od['Price']['Tax']['CityName'],
                            'tax_county_name': od['Price']['Tax']['CountyName'],
                            'tax_state_name': od['Price']['Tax']['StateName'],
                            'tax_zip': od['Price']['Tax']['Zip'],
                            'department_id': od['Department']['ID']['_value_1'],
                            'department_name': od['Department']['Name'],
                            'department_number': od['Department']['Number'],
                            'supplier_work_order_id': (od['SupplierWorkOrder']['ID']['_value_1'] if
                                                       od['SupplierWorkOrder'] is not None else None),
                            'supplier_work_order_name': (od['SupplierWorkOrder']['Name'] if
                                                         od['SupplierWorkOrder'] is not None else None),
                            'supplier_id': od['Supplier']['ID']['_value_1'],
                            'shipping_date': od['Shipping']['Date'],
                            'shipping_date_shipped': od['Shipping']['DateShipped'],
                            'shipping_method': (ascii_replace.sub('', str(od['Shipping']['Method'])) if
                                                od['Shipping']['Method'] is not None else None),
                            'shipping_instructions': od['Shipping']['Instructions'],
                            'shipping_address_id': od['Shipping']['Address']['ID']['_value_1'],
                            'shipping_tracking': (od['Shipping']['TrackingNumber'][0:12] if
                                                  od['Shipping']['TrackingNumber'] is not None else None),
                            'shipping_tax': od['Shipping']['Tax'],
                            'postage_cost': (od['Postage']['Cost']['_value_1'] if
                                             od['Postage'] is not None else None),
                            'postage_method': (od['Postage']['Method'] if
                                               od['Postage'] is not None else None),
                            'client_status_value': od['ClientStatus']['Value'],
                            'client_status_date': od['ClientStatus']['Date'],
                            'seller_status_value': od['SellerStatus']['Value'],
                            'seller_status_date': od['SellerStatus']['Date'],
                            'supplier_status_value': od['SupplierStatus']['Value'],
                            'supplier_status_date': od['SupplierStatus']['Date'],

                            'credit_card_settlement': (settings.clean_json(od['CreditCardSettlement']) if
                                                       od['CreditCardSettlement'] is not None else None),

                            'kit': (settings.clean_json(od['Kit']['KitDetail']) if
                                    od['Kit'] is not None else None),

                            'order_order_id': od['OrderID']['_value_1'],
                            'order_order_number': od['OrderNumber'],
                            'client_po': od['ClientPONumber'],
                            'custom_work_order_fields': od['CustomOrderFields'],
                            'sales_work_order_id': od['SalesWorkOrderID']['_value_1'],
                            'product_type': od['ProductType'],
                            'list_vendor': od['ListVendor'],
                            'finishing_options': (settings.clean_json(od['FinishingOptions']) if
                                                  od['FinishingOptions'] is not None else None),
                            'coupons': od['Coupons'],
                            'attached_files': (settings.clean_json(od['AttachedFiles']) if
                                               od['AttachedFiles'] is not None else None),
                            'uploaded_files': (settings.clean_json(od['UploadedFiles']) if
                                               od['UploadedFiles'] is not None else None),
                            'sku_inventory_settings': od['SKUInventorySettings'],
                            'pagecount': od['PageCount'],
                            'catalog_tree_node_id': od['CatalogTreeNodeExternalId'],
                            'job_direct_options': od['JobDirectOptions'],
                            'impersonator_fields': (settings.clean_json(od['Impersonator']) if
                                                    od['Impersonator'] is not None else None),
                            'requisition_status': od['RequisitionStatus'],
                            'approver_user': od['ApproverUser'],
                            'explanation': od['Explanation'],
                            'job_direct_settings': od['JobDirectSettings']
                            })


class OrderDetailUsers:
    # create a order_detail_users list object for WebUser table from the order details
    def __init__(self):
        self.groups = []

    def append_to_group(self, od, elem):
        self.groups.append({'user_id': od['User']['ID']['_value_1'],
                            'fname': od['User']['FirstName'],
                            'lname': od['User']['LastName'],
                            'login_id': od['User']['LoginID'],
                            'email': od['User']['Email'],
                            'generic_user_fields': (settings.clean_json(od['User']['GenericUserFields']) if
                                                    od['User']['GenericUserFields'] is not None else None),
                            'update_date': datetime.datetime.strftime(elem['CreateDate'], "%Y-%m-%d %H:%M:%S"),
                            })


class SalesWork:
    # create a sales_work list object for SalesWorkOrder table
    def __init__(self):
        self.groups = []

    def append_to_group(self, order_id, swo):
        self.groups.append({'order_id': order_id,
                            'sales_work_order_id': swo['ID']['_value_1'],
                            'order_number': swo['OrderNumber'],
                            'supplier_name': swo['SupplierName'],
                            'credit_card': (settings.clean_json(swo['CreditCard']) if
                                            swo['CreditCard'] is not None else None),
                            'handling': swo['Handling']
                            })


class ShipAddr(dict):
    # create a ship_addr dictionary object for BillingAddress table
    def init_values(self, elem):
        self['id'] = elem['ShippingAddress']['ID']['_value_1']
        self['description'] = elem['ShippingAddress']['Description']
        self['address1'] = elem['ShippingAddress']['Address1']
        self['address2'] = elem['ShippingAddress']['Address2']
        self['address3'] = elem['ShippingAddress']['Address3']
        self['city'] = elem['ShippingAddress']['City']
        self['state'] = elem['ShippingAddress']['State']
        self['zip'] = elem['ShippingAddress']['Zip']
        self['country'] = elem['ShippingAddress']['Country']
        self['phone'] = elem['ShippingAddress']['PhoneNumber']
        self['fax'] = elem['ShippingAddress']['FaxNumber']
        self['company_name'] = elem['ShippingAddress']['CompanyName']
        self['attn'] = elem['ShippingAddress']['Attn']
        self['email'] = elem['ShippingAddress']['Email']
        self['type'] = elem['ShippingAddress']['Type']
        self['default_addr'] = (1 if (elem['ShippingAddress']['Default']) == 'True' else 0)


class BillAddr(dict):
    # create a ship_addr dictionary object for BillingAddress table
    def init_values(self, elem):
        self['id'] = elem['BillingAddress']['ID']['_value_1']
        self['description'] = elem['BillingAddress']['Description']
        self['address1'] = elem['BillingAddress']['Address1']
        self['address2'] = elem['BillingAddress']['Address2']
        self['address3'] = elem['BillingAddress']['Address3']
        self['city'] = elem['BillingAddress']['City']
        self['state'] = elem['BillingAddress']['State']
        self['zip'] = elem['BillingAddress']['Zip']
        self['country'] = elem['BillingAddress']['Country']
        self['phone'] = elem['BillingAddress']['PhoneNumber']
        self['fax'] = elem['BillingAddress']['FaxNumber']
        self['company_name'] = elem['BillingAddress']['CompanyName']
        self['attn'] = elem['BillingAddress']['Attn']
        self['email'] = elem['BillingAddress']['Email']
        self['type'] = elem['BillingAddress']['Type']
        self['default_addr'] = (1 if (elem['BillingAddress']['Default']) == 'True' else 0)


class Company(dict):
    # create a company dictionary object for Company table
    def init_values(self, elem):
        self['company_id'] = elem['Company']['ID']['_value_1']
        self['company_name'] = elem['Seller']['Name']


class Seller(dict):
    # create a seller dictionary object for Seller table
    def init_values(self, elem):
        self['seller_id'] = elem['Seller']['ID']['_value_1']
        self['seller_name'] = elem['Seller']['Name']


class User(dict):
    # create a user dictionary object for WebUser table
    def init_values(self, elem):
        self['user_id'] = elem['User']['ID']['_value_1']
        self['fname'] = elem['User']['FirstName']
        self['lname'] = elem['User']['LastName']
        self['login_id'] = elem['User']['LoginID']
        self['generic_user_fields'] = ""
        self['email'] = elem['User']['Email']
        self['update_date'] = datetime.datetime.strftime(elem['CreateDate'], "%Y-%m-%d %H:%M:%S")
        self['user_group'] = (settings.clean_json(elem['UserGroups']['UserGroup']) if
                              elem['UserGroups']['UserGroup'] is not None else None)


class Record(dict):
    # create a rec dictionary object for OrderByRequest table
    def init_values(self, elem, token, fetch_date):
        self['order_id'] = elem['ID']['_value_1']
        self['order_number'] = elem['OrderNumber']
        self['order_description'] = elem['Description']

        self['create_date'] = elem['CreateDate']
        self['create_date_pst'] = pytz.timezone('America/Los_Angeles').localize(elem['CreateDate'])

        self['status'] = elem['Status']
        self['user_id'] = elem['User']['ID']['_value_1']
        self['user_group_syncmode'] = elem['UserGroups']['SyncMode']
        self['user_group_all_or_none'] = elem['UserGroups']['AllOrNone']
        self['seller_id'] = elem['Seller']['ID']['_value_1']
        self['company_id'] = elem['Company']['ID']['_value_1']
        self['supplier'] = elem['Supplier']
        self['billing_address_id'] = elem['BillingAddress']['ID']['_value_1']
        self['shipping_address_id'] = elem['ShippingAddress']['ID']['_value_1']
        self['payment_method'] = elem['PaymentMethod']
        self['payment_method_detail'] = elem['PaymentMethodDetail']
        self['credit_card'] = (settings.clean_json(elem['CreditCard']) if
                               elem['CreditCard'] is not None else None)
        self['attached_files'] = elem['AttachedFiles']
        self['token'] = token
        self['fetch_date'] = fetch_date


def processing_files_table(gblv, orders):
    print("Creating processing file table")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM `ProcessingFiles` WHERE `filename` IS NOT NULL ;")
    conn.commit()

    # orders = [f for f in os.listdir(gblv.downloaded_orders_path) if f[-3:].upper() == 'DAT']

    for order in orders:
        sql = ("INSERT INTO `ProcessingFiles` (filename, order_datetime_utc, order_datetime_pst,"
               "user_id) VALUES (?,?,?,?);")

        parse_filename = str.split(order, '_')

        userid = parse_filename[0]
        order_datetime_utc = datetime.datetime.strptime(parse_filename[1][:-4], "%Y%m%d%H%M%S")
        order_datetime_utc = order_datetime_utc.replace(tzinfo=pytz.utc)
        order_datetime_pst = order_datetime_utc.astimezone(pytz.timezone('America/Los_Angeles'))

        cursor.execute(sql, (order, order_datetime_utc, order_datetime_pst, userid))

    conn.commit()
    conn.close()


def no_match_files_table(gblv, orders):
    print("Creating no match file table")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM `NoMatchFiles` WHERE `filename` IS NOT NULL ;")
    conn.commit()

    # orders = [f for f in os.listdir(gblv.downloaded_orders_path) if f[-3:].upper() == 'DAT']

    for order in orders:
        sql = ("INSERT INTO `NoMatchFiles` (filename, order_datetime_utc, order_datetime_pst,"
               "user_id) VALUES (?,?,?,?);")

        parse_filename = str.split(order, '_')

        userid = parse_filename[0]
        order_datetime_utc = datetime.datetime.strptime(parse_filename[1][:-4], "%Y%m%d%H%M%S")
        order_datetime_utc = order_datetime_utc.replace(tzinfo=pytz.utc)
        order_datetime_pst = order_datetime_utc.astimezone(pytz.timezone('America/Los_Angeles'))

        cursor.execute(sql, (order, order_datetime_utc, order_datetime_pst, userid))

    conn.commit()
    conn.close()


def update_no_order_file_table(fle, eddm_order, gblv):
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()

    sql1 = "UPDATE `NoMatchFiles` SET order_records = ? WHERE filename = ?;"
    sql2 = "UPDATE `NoMatchFiles` SET order_file_touches = ? WHERE filename = ?;"

    cursor.execute(sql1, (eddm_order.file_qty, fle,))
    cursor.execute(sql2, (eddm_order.file_touches, fle,))

    conn.commit()
    conn.close()


def update_processing_file_table(fle, eddm_order, gblv):
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()

    sql1 = "UPDATE `ProcessingFiles` SET order_records = ? WHERE filename = ?;"
    sql2 = "UPDATE `ProcessingFiles` SET order_file_touches = ? WHERE filename = ?;"

    cursor.execute(sql1, (eddm_order.file_qty, fle,))
    cursor.execute(sql2, (eddm_order.file_touches, fle,))

    conn.commit()
    conn.close()


def clear_file_history_table(gblv):
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    sql = "DELETE FROM FileHistory WHERE `filename` IS NOT null;"
    cursor.execute(sql)
    conn.commit()
    conn.close()


def clear_processing_files_table(gblv):
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    sql = "DELETE FROM ProcessingFiles WHERE `filename` IS NOT null;"
    cursor.execute(sql)
    conn.commit()
    conn.close()


def vacuum_database(gblv):
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    sql = "VACUUM;"
    cursor.execute(sql)
    conn.commit()
    conn.close()


def update_order_touches_table(gblv):
    """
    Creates a table of records that need to be updated in the EDDM database to 
    match the number of touches to the Marcom number of touches
    """
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()

    sql = "DROP TABLE IF EXISTS `update_touch_records`;"
    cursor.execute(sql)

    sql = ("CREATE TABLE `update_touch_records` ("
           "`filename` VARCHAR(100) NOT NULL,"
           "`agent_id` VARCHAR(10) NULL DEFAULT NULL,"
           "`date_selected` DATETIME NULL DEFAULT NULL,"
           "`city` VARCHAR(60) DEFAULT NULL,"
           "`state` VARCHAR(2) DEFAULT NULL,"
           "`zipcode` VARCHAR(5) DEFAULT NULL,"
           "`routeid` VARCHAR(5) DEFAULT NULL,"
           "`quantity` int(8) DEFAULT NULL,"
           "`pos` int(8) DEFAULT NULL,"
           "`number_of_touches` int(8) DEFAULT NULL,"
           "`session_id` VARCHAR(50) DEFAULT NULL);")

    cursor.execute(sql)
    conn.commit()
    conn.close()


def insert_into_update_order_touches_table(gblv, filename, rec):
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    sql = ("INSERT INTO `update_touch_records` (`filename`,"
           "`agent_id`,`date_selected`,`city`,`state`,`zipcode`,"
           "`routeid`,`quantity`,`pos`,`number_of_touches`, `session_id`) "
           "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")

    # 7/16/2019 7:11:10 PM
    date_selected = datetime.datetime.strptime(rec['DateSelected'], "%m/%d/%Y %I:%M:%S %p")

    cursor.execute(sql, (filename, rec['AgentID'], date_selected, 
                         rec['City'], rec['State'], rec['ZipCode'], rec['RouteID'], 
                         rec['Quantity'], rec['POS'], rec['NumberOfTouches'],
                         rec['SessionID']))
    conn.commit()
    conn.close()


def qry_processing_files_history(gblv, jobname):
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()

    sql = ("SELECT count(*), filename, jobname, order_records FROM `ProcessingFilesHistory` "
           "WHERE jobname LIKE '{}%';".format(jobname))

    cursor.execute(sql)
    results = cursor.fetchall()

    conn.commit()
    conn.close()

    return results


def delete_orders_table(gblv):
    """
    Creates a table of all the records that are being released for processing again,
    for files that are older than 48 hours.
    """
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()

    sql = "DROP TABLE IF EXISTS `delete_order_records`;"
    cursor.execute(sql)

    sql = ("CREATE TABLE `delete_order_records` ("
           "`filename` VARCHAR(100) NOT NULL,"
           "`agent_id` VARCHAR(10) NULL DEFAULT NULL,"
           "`date_selected` DATETIME NULL DEFAULT NULL,"
           "`city` VARCHAR(60) DEFAULT NULL,"
           "`state` VARCHAR(2) DEFAULT NULL,"
           "`zipcode` VARCHAR(5) DEFAULT NULL,"
           "`routeid` VARCHAR(5) DEFAULT NULL,"
           "`quantity` int(8) DEFAULT NULL,"
           "`pos` int(8) DEFAULT NULL,"
           "`number_of_touches` int(8) DEFAULT NULL,"
           "`session_id` VARCHAR(50) DEFAULT NULL);")

    cursor.execute(sql)
    conn.commit()
    conn.close()


def insert_into_delete_orders_table(gblv, filename, rec):
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    sql = ("INSERT INTO `delete_order_records` (`filename`,"
           "`agent_id`,`date_selected`,`city`,`state`,`zipcode`,"
           "`routeid`,`quantity`,`pos`,`number_of_touches`, `session_id`) "
           "VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")

    # 7/16/2019 7:11:10 PM
    date_selected = datetime.datetime.strptime(rec['DateSelected'], "%m/%d/%Y %I:%M:%S %p")

    cursor.execute(sql, (filename, rec['AgentID'], date_selected, 
                         rec['City'], rec['State'], rec['ZipCode'], rec['RouteID'], 
                         rec['Quantity'], rec['POS'], rec['NumberOfTouches'],
                         rec['SessionID']))
    conn.commit()
    conn.close()


def import_userdata(gblv):
    """
    Imports V2FBLUSERDATA.txt file from path, used to determine if user is still active
    """
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()

    sql = "DROP TABLE IF EXISTS `v2fbluserdata`;"
    cursor.execute(sql)
    conn.commit()

    sql = ("CREATE TABLE `v2fbluserdata` ("
           "`agent_id` VARCHAR(10) NOT NULL,"
           "`nickname` VARCHAR(60) DEFAULT NULL,"
           "`fname` VARCHAR(60) DEFAULT NULL,"
           "`lname` VARCHAR(60) DEFAULT NULL,"
           "`cancel_date` DATETIME NULL DEFAULT NULL,"
           "`file_update_date` DATETIME NULL DEFAULT NULL,"
           "PRIMARY KEY (`agent_id`));")
    cursor.execute(sql)

    with open(gblv.user_data_path, 'r') as users:
        for n, line in enumerate(users):
            agentid = line[2:7]
            nickname = line[33:93].strip()
            fname = line[93:153].strip()
            lname = line[213:273].strip()
            cancel_date = (datetime.datetime.strptime(line[386:394], '%Y%m%d')
                           if not line[386:394] == '00000000' else None)

            sql = "INSERT INTO `v2fbluserdata` VALUES (?,?,?,?,?, datetime('now', 'localtime'));"
            cursor.execute(sql, (agentid, nickname, fname, lname, cancel_date,))

    conn.commit()


def append_filename_to_orderdetail(gblv):
    sql = ("UPDATE OrderDetail SET `file_match` = "
           "(SELECT filename FROM ProcessingFiles WHERE jobname = "
           'TRIM((OrderDetail.order_order_number||"_"||OrderDetail.order_detail_id))) '
           "WHERE EXISTS (SELECT filename FROM ProcessingFiles WHERE "
           'jobname = TRIM((OrderDetail.order_order_number||"_"||OrderDetail.order_detail_id))) ;')

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


def append_filename_to_orderdetail_48_hour(gblv):
    sql = ("UPDATE OrderDetail SET `file_match` = "
           "(SELECT filename FROM NoMatchFiles WHERE jobname = "
           'TRIM((OrderDetail.order_order_number||"_"||OrderDetail.order_detail_id))) '
           "WHERE EXISTS (SELECT filename FROM NoMatchFiles WHERE "
           'jobname = TRIM((OrderDetail.order_order_number||"_"||OrderDetail.order_detail_id))) ;')

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql)
    conn.commit()
    conn.close()


def processing_table_to_history(gblv):
    """Backs up ProcessingFiles table to ProcessingFilesHistory"""
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute("REPLACE INTO `ProcessingFilesHistory` SELECT * FROM `ProcessingFiles`;")
    conn.commit()
    conn.close()


def update_file_history_table(gblv, **insert_values):

    sql = ("REPLACE INTO `FileHistory` VALUES ("
           "?, ?, DATE(?), ?, ?, ?, DATE(?), ?);")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (insert_values['filename'], 
                         insert_values['jobname'], 
                         insert_values['processing_date'],
                         insert_values['order_records'], 
                         insert_values['total_touches'], 
                         insert_values['touch'], 
                         insert_values['mailing_date'], 
                         insert_values['user_id']))

    conn.commit()
    conn.close()


def status_update_processing_file_table(gblv, filename, message):

    sql = ("UPDATE `ProcessingFiles` SET `status` = ? "
           "where `filename` = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (message, filename))
    conn.commit()
    conn.close()


def status_update_processing_history_table(gblv, filename, message):

    sql = ("UPDATE `ProcessingFilesHistory` SET `status` = ? "
           "where `filename` = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (message, filename))
    conn.commit()
    conn.close()


def cancel_order_detail_order(gblv, order_number, message):

    sql = ("UPDATE `OrderDetail` SET `file_match` = ? "
           "where `order_order_number` = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (message, order_number))
    status = cursor.rowcount
    conn.commit()
    conn.close()
    return status


def status_update_processing_no_match_table(gblv, filename, message):

    sql = ("UPDATE `NoMatchFiles` SET `status` = ? "
           "where `filename` = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (message, filename))
    conn.commit()
    conn.close()


def v2fbluserdata_update_date(gblv):
    sql = "SELECT strftime('%Y-%m-%d %H:%m:%S', file_update_date) FROM v2fbluserdata LIMIT 1;"
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()

    conn.commit()
    conn.close()

    return results


def count_unmatched_orders_order_detail(gblv):
    sql = "SELECT count(*) FROM orderdetail WHERE file_match IS NULL;"
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()

    conn.commit()
    conn.close()

    return results


def nomatch_processing_files_log(gblv):
    sql = ('SELECT * FROM (SELECT a.filename, IFNULL(b.jobname, "")'
           ", DATETIME(a.order_datetime_utc, 'localtime'), "
           "a.order_records, a.order_file_touches, IFNULL(a.marcom_records, 0), "
           "IFNULL(a.marcom_order_touches, 0), a.status, "
           "IFNULL(b.mailing_date, '') FROM NomatchFiles a LEFT JOIN "
           "filehistory b ON substr(a.jobname, 1, 17) = substr(b.jobname, 1, 17) "
           "ORDER BY a.order_datetime_utc ASC);")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql)

    results = cursor.fetchall()

    conn.commit()
    conn.close()

    return results


def processing_files_log(gblv):
    sql = ('SELECT * FROM (SELECT a.filename, IFNULL(b.jobname, "")'
           ", DATETIME(a.order_datetime_utc, 'localtime'), "
           "a.order_records, a.order_file_touches, IFNULL(a.marcom_records, 0), "
           "IFNULL(a.marcom_order_touches, 0), a.status, IFNULL(b.mailing_date, '') "
           "FROM processingfiles a LEFT JOIN filehistory b ON "
           "substr(a.jobname, 1, 17) = substr(b.jobname, 1, 17) "
           "WHERE b.mailing_date is not null ORDER BY b.mailing_date ASC) "
           "UNION ALL "
           'SELECT * FROM (SELECT a.filename, IFNULL(b.jobname, "")'
           ", DATETIME(a.order_datetime_utc, 'localtime'), "
           "a.order_records, a.order_file_touches, IFNULL(a.marcom_records, 0), "
           "IFNULL(a.marcom_order_touches, 0), a.status, "
           "IFNULL(b.mailing_date, '') FROM processingfiles a LEFT JOIN "
           "filehistory b ON substr(a.jobname, 1, 17) = substr(b.jobname, 1, 17) "
           "WHERE b.mailing_date is null ORDER BY a.order_datetime_utc ASC) ;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql)

    results = cursor.fetchall()

    conn.commit()
    conn.close()

    return results


def delete_order_record_unlock_routes(gblv, session_id_set):

    with pytds.connect(gblv.mssql_connection, 
                       gblv.mssql_database, 
                       gblv.mssql_user, 
                       gblv.mssql_pass) as conn:

        with conn.cursor() as cur:
            for sess_id in session_id_set:
                sql = "EXEC guideone.EDDM_DeleteSelectedRoutes '{}';".format(sess_id)
                # print(sql)
                cur.execute(sql)

        conn.commit()


def order_submit_update_route_touches(gblv, session_id_set):

    with pytds.connect(gblv.mssql_connection, 
                       gblv.mssql_database, 
                       gblv.mssql_user, 
                       gblv.mssql_pass) as conn:

        with conn.cursor() as cur:
            for sess_id in session_id_set:
                sql = "EXEC guideone.EDDM_SwapNumberOfDrops '{}';".format(sess_id)
                cur.execute(sql)
        conn.commit()


def get_session_id_sqlite(gblv, table):
    sql = ("SELECT `session_id` FROM `{}` GROUP BY `session_id`;".format(table))

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql)

    results = cursor.fetchall()

    conn.commit()
    conn.close()

    session_id_set = set()
    for rec in results:
        session_id_set.add(rec[0])

    return session_id_set


def update_touch_record_session_ids(gblv):
    """DEPRECIATED, NOT USED.  SEE FUNCTION: get_session_id_sqlite"""
    sql = "SELECT agent_id, date_selected FROM update_touch_records;"

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    conn.close()

    session_id_set = set()
    for agentid, date_selected in results:
        with pytds.connect(gblv.mssql_connection, 
                           gblv.mssql_database, 
                           gblv.mssql_user, 
                           gblv.mssql_pass) as conn:

            with conn.cursor() as cur:
                mssql = ("SELECT a.* FROM guideone.UserEDDMRoute a "
                         "JOIN guideone.EDDMUser b ON "
                         "a.EDDMUserId = b.EDDMUserId "
                         "WHERE b.UserName = '{}' AND "
                         "DATEADD(ms, -DATEPART(ms, a.DateSelected), a.DateSelected) "
                         "= '{}';".format(agentid, date_selected)) 

                cur.execute(mssql)

                for rec in cur.fetchall():
                    session_id_set.add(rec[6])

    return session_id_set


def delete_order_record_session_ids(gblv):
    """DEPRECIATED, NOT USED.  SEE FUNCTION: get_session_id_sqlite"""
    sql = "SELECT agent_id, date_selected FROM delete_order_records;"

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql)
    results = cursor.fetchall()
    conn.close()

    session_id_set = set()
    for agentid, date_selected in results:
        with pytds.connect(gblv.mssql_connection, 
                           gblv.mssql_database, 
                           gblv.mssql_user, 
                           gblv.mssql_pass) as conn:

            with conn.cursor() as cur:
                mssql = ("SELECT a.* FROM guideone.UserEDDMRoute a "
                         "JOIN guideone.EDDMUser b ON "
                         "a.EDDMUserId = b.EDDMUserId "
                         "WHERE b.UserName = '{}' AND "
                         "DATEADD(ms, -DATEPART(ms, a.DateSelected), a.DateSelected) "
                         "= '{}';".format(agentid, date_selected)) 

                cur.execute(mssql)

                for rec in cur.fetchall():
                    session_id_set.add(rec[6])

    return session_id_set


def jobs_mailing_agent_status(gblv, days):
    """
    Runs a query that will show the active status of agents
    with jobs mailing in the next [days] from today
    """
    sql = ("SELECT a.jobname, a.mailing_date, a.user_id, "
           "b.agent_id, case when b.cancel_date is null "
           "then 'ACTIVE' else 'INACTIVE' END "
           ', (b.nickname||" "||b.lname) FROM FileHistory a '
           "JOIN v2fbluserdata b ON a.user_id = b.agent_id WHERE "
           "cast((julianday(a.mailing_date) - julianday(date('now', 'localtime'))) "
           "as INTEGER ) BETWEEN 0 AND ? ORDER BY a.mailing_date ASC;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (days,))

    results = cursor.fetchall()

    conn.commit()
    conn.close()

    return results


def extended_update_processing_file_table(gblv, filename, eddm_order):

    sql1 = ("UPDATE `ProcessingFiles` SET `order_processed_utc` = strftime('%Y-%m-%d %H:%M:%S+00:00') "
            "where `filename` = ?;")

    sql2 = ("UPDATE `ProcessingFiles` SET `marcom_records` = ? "
            "where `filename` = ?;")

    sql3 = ("UPDATE `ProcessingFiles` SET `marcom_order_touches` = ? "
            "where `filename` = ?;")

    sql4 = ("UPDATE `ProcessingFiles` SET `jobname` = ? "
            "where `filename` = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql1, (filename,))
    cursor.execute(sql2, (eddm_order.order_qty, filename,))
    cursor.execute(sql3, (eddm_order.order_touches, filename,))
    cursor.execute(sql4, (eddm_order.jobname, filename,))

    conn.commit()
    conn.close()


def extended_update_no_match_table(gblv, filename, eddm_order):

    sql1 = ("UPDATE `NoMatchFiles` SET `order_processed_utc` = strftime('%Y-%m-%d %H:%M:%S+00:00') "
            "where `filename` = ?;")

    sql2 = ("UPDATE `NoMatchFiles` SET `marcom_records` = ? "
            "where `filename` = ?;")

    sql3 = ("UPDATE `NoMatchFiles` SET `marcom_order_touches` = ? "
            "where `filename` = ?;")

    sql4 = ("UPDATE `NoMatchFiles` SET `jobname` = ? "
            "where `filename` = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql1, (filename,))
    cursor.execute(sql2, (eddm_order.order_qty, filename,))
    cursor.execute(sql3, (eddm_order.order_touches, filename,))
    cursor.execute(sql4, (eddm_order.jobname, filename,))

    conn.commit()
    conn.close()


def no_match_to_order_hard_match(fle, gblv, min_diff=120):
    """
    Returns true if there is a hard match.  The date in the file,
    the number of records all match with the order data from the API.
    The number of touches does not need to match, but does need to be populated.  
    The date of the order data and the file data are within min_diff of each other.
    """
    sql = ("SELECT count(), a.filename "
           ", c.order_number||'_'||b.order_detail_id 'job number', "
           "a.order_datetime_utc 'file utc', a.order_datetime_pst 'file pst', "
           "c.create_date_pst 'order pst', a.order_records 'file records', "
           "a.order_file_touches 'file touches', a.user_id 'file user id', "
           "b.eddm_touches 'order touches', "
           "abs(cast((julianday(a.order_datetime_pst) - "
           "julianday(c.create_date_pst)) * 24 * 60 as INTEGER )) 'min diff' "
           ", b.quantity 'order qty'"
           "FROM NoMatchFiles a JOIN OrderDetail b ON a.user_id = b.user_id "
           "AND a.order_records = b.quantity "
           "JOIN OrderRequestByDate c ON b.order_id = c.order_id "
           'WHERE "min diff" <= ? '
           "AND c.order_number||'_'||b.order_detail_id NOT IN (SELECT substr(jobname, 1, 17) from FileHistory) "
           "AND a.filename = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (min_diff, fle,))

    ans = cursor.fetchone()
    result = (ans[0] != 0, ans)

    conn.commit()
    conn.close()

    return result


def no_match_to_order_hard_previous_match(fle, gblv, min_diff=120):
    """
    Returns true if there is a hard match.  The date in the file,
    the number of records all match with the order data from the API.
    The number of touches does not need to match, but does need to be populated.
    The date of the order data and the file data are within min_diff of each other.
    """
    sql = ("SELECT count(), a.filename, c.order_number||'_'||b.order_detail_id 'job number', "
           "abs(cast((julianday(a.order_datetime_pst) - "
           "julianday(c.create_date_pst)) * 24 * 60 as INTEGER )) 'min diff' "
           "FROM NoMatchFiles a JOIN OrderDetail b ON a.user_id = b.user_id "
           "AND a.order_records = b.quantity "
           "JOIN OrderRequestByDate c ON b.order_id = c.order_id "
           'WHERE "min diff" <= ? '
           "AND c.order_number||'_'||b.order_detail_id IN (SELECT substr(jobname, 1, 17) from FileHistory) "
           "AND a.filename = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (min_diff, fle,))

    ans = cursor.fetchone()
    result = (ans[0] != 0, ans)

    conn.commit()
    conn.close()

    return result


def no_match_to_order_previous_match(fle, gblv, min_diff=120):
    """
    Returns true if there is a match to a previous job.  The date matches,
    the number of does not match with the order data from the API.
    There has been a previously existing job match.
    The number of touches does not need to match, but does need to be populated.  
    The date of the order data and the file data are within min_diff of each other.
    """
    sql = ("SELECT count(), a.filename "
           ", c.order_number||'_'||b.order_detail_id 'job number', "
           "a.order_datetime_utc 'file utc', a.order_datetime_pst 'file pst', "
           "c.create_date_pst 'order pst', a.order_records 'file records', "
           "a.order_file_touches 'file touches', a.user_id 'file user id', "
           "b.eddm_touches 'order touches', "
           "abs(cast((julianday(a.order_datetime_pst) - "
           "julianday(c.create_date_pst)) * 24 * 60 as INTEGER )) 'min diff' "
           ", b.quantity 'order qty'"
           "FROM NoMatchFiles a JOIN OrderDetail b ON a.user_id = b.user_id "
           "JOIN OrderRequestByDate c ON b.order_id = c.order_id "
           'WHERE "min diff" <= ? '
           "AND c.order_number||'_'||b.order_detail_id IN (SELECT substr(jobname, 1, 17) from FileHistory) "
           "AND a.filename = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (min_diff, fle,))

    ans = cursor.fetchone()
    result = (ans[0] != 0, ans)

    conn.commit()
    conn.close()

    return result


def no_match_to_order_soft_match(fle, gblv, min_diff=120):
    """
    Returns true if there is a soft match.  The date in the matches,
    the number of does not match with the order data from the API.
    There can not be a previously existing job match.
    The number of touches does not need to match, but does need to be populated.  
    The date of the order data and the file data are within min_diff of each other.
    """
    sql = ("SELECT count(), a.filename "
           ", c.order_number||'_'||b.order_detail_id 'job number', "
           "a.order_datetime_utc 'file utc', a.order_datetime_pst 'file pst', "
           "c.create_date_pst 'order pst', a.order_records 'file records', "
           "a.order_file_touches 'file touches', a.user_id 'file user id', "
           "b.eddm_touches 'order touches', "
           "abs(cast((julianday(a.order_datetime_pst) - "
           "julianday(c.create_date_pst)) * 24 * 60 as INTEGER )) 'min diff' "
           ", b.quantity 'order qty'"
           "FROM NoMatchFiles a JOIN OrderDetail b ON a.user_id = b.user_id "
           "JOIN OrderRequestByDate c ON b.order_id = c.order_id "
           'WHERE "min diff" <= ? '
           "AND c.order_number||'_'||b.order_detail_id NOT IN (SELECT substr(jobname, 1, 17) from FileHistory) "
           "AND a.filename = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (min_diff, fle,))

    ans = cursor.fetchone()
    result = (ans[0] != 0, ans)

    conn.commit()
    conn.close()

    return result


def file_to_order_hard_match(fle, gblv, min_diff=120):
    """
    Returns true if there is a hard match.  The date in the file,
    the number of records all match with the order data from the API.
    The number of touches does not need to match, but does need to be populated.  
    The date of the order data and the file data are within min_diff of each other.
    """
    sql = ("SELECT count(), a.filename "
           ", c.order_number||'_'||b.order_detail_id 'job number', "
           "a.order_datetime_utc 'file utc', a.order_datetime_pst 'file pst', "
           "c.create_date_pst 'order pst', a.order_records 'file records', "
           "a.order_file_touches 'file touches', a.user_id 'file user id', "
           "b.eddm_touches 'order touches', "
           "abs(cast((julianday(a.order_datetime_pst) - "
           "julianday(c.create_date_pst)) * 24 * 60 as INTEGER )) 'min diff' "
           ", b.quantity 'order qty'"
           "FROM ProcessingFiles a JOIN OrderDetail b ON a.user_id = b.user_id "
           "AND a.order_records = b.quantity "
           "JOIN OrderRequestByDate c ON b.order_id = c.order_id "
           'WHERE "min diff" <= ? '
           "AND c.order_number||'_'||b.order_detail_id NOT IN (SELECT substr(jobname, 1, 17) from FileHistory) "
           "AND a.filename = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (min_diff, fle,))

    ans = cursor.fetchone()
    result = (ans[0] != 0, ans)

    conn.commit()
    conn.close()

    return result


def marcom_orders_unmatched(gblv):
    sql = ("SELECT strftime('%m/%d/%Y %H:%M:%S', b.create_date, '+2 hour') "
           "'order date', a.user_id 'user id', a.order_id 'order id', "
           "a.order_detail_id 'order detail id', "
           "a.order_order_number 'order number', a.quantity 'qty' "
           "FROM OrderDetail a JOIN OrderRequestByDate b "
           "ON a.order_id = b.order_id WHERE file_match IS NULL ;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql)

    results = cursor.fetchall()

    conn.commit()
    conn.close()

    return results


def file_to_order_force_match(fle, order_detail_order_id, gblv):
    """
    Forces a match based on file name.  A matching order must be 
    in the OrderRequestByDate, Processingfiles, and OrderDetail tables.
    Will overwrite tables if previously matched to FileHistory.
    Run with extreme caution!
    """
    sql = ("SELECT count(), a.filename , c.order_number||'_'||b.order_detail_id"
           " 'job number', a.order_datetime_utc 'file utc', "
           "a.order_datetime_pst 'file pst', c.create_date_pst 'order pst', "
           "a.order_records 'file records', a.order_file_touches 'file touches', "
           "a.user_id 'file user id', b.eddm_touches 'order touches', "
           "abs(cast((julianday(a.order_datetime_pst) - "
           "julianday(c.create_date_pst)) * 24 * 60 as INTEGER )) 'min diff', "
           "b.quantity 'order qty'FROM ProcessingFiles a JOIN OrderDetail b "
           "ON a.user_id = b.user_id AND a.order_records = b.quantity "
           "JOIN OrderRequestByDate c ON b.order_id = c.order_id "
           "WHERE a.filename = ? AND b.order_detail_id = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (fle, order_detail_order_id))

    ans = cursor.fetchone()
    result = (ans[0] != 0, ans)

    conn.commit()
    conn.close()

    return result


def clean_unused_order_detail(gbl):
    """
    Deletes orders from OrderDetail that are not product_id = 853 (EDDM Orders)
    """
    # Make a sql connection
    print("Cleaning up database")
    conn = sqlite3.connect(gbl.db_name)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM OrderDetail WHERE product_id != '853' AND product_id != '3029';")
    conn.commit()
    conn.close()


def clean_unused_orders(gblv):
    """
    Removes records from OrderRequestByDate that are not on OrderDetail (EDDM Orders)
    """
    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()

    cursor.execute("DELETE FROM `OrderRequestByDate` "
                   "WHERE NOT EXISTS (SELECT * FROM `OrderDetail` "
                   "WHERE `OrderDetail`.order_id = "
                   "`OrderRequestByDate`.order_id);")
    
    conn.commit()
    conn.close()


def file_to_order_hard_previous_match(fle, gblv, min_diff=120):
    """
    Returns true if there is a hard match.  The date in the file,
    the number of records all match with the order data from the API.
    The number of touches does not need to match, but does need to be populated.
    The date of the order data and the file data are within min_diff of each other.
    """
    sql = ("SELECT count(), a.filename, c.order_number||'_'||b.order_detail_id 'job number', "
           "abs(cast((julianday(a.order_datetime_pst) - "
           "julianday(c.create_date_pst)) * 24 * 60 as INTEGER )) 'min diff' "
           "FROM ProcessingFiles a JOIN OrderDetail b ON a.user_id = b.user_id "
           "AND a.order_records = b.quantity "
           "JOIN OrderRequestByDate c ON b.order_id = c.order_id "
           'WHERE "min diff" <= ? '
           "AND c.order_number||'_'||b.order_detail_id IN (SELECT substr(jobname, 1, 17) from FileHistory) "
           "AND a.filename = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (min_diff, fle,))

    ans = cursor.fetchone()
    result = (ans[0] != 0, ans)

    conn.commit()
    conn.close()

    return result


def file_to_order_previous_match(fle, gblv, min_diff=120):
    """
    Returns true if there is a match to a previous job.  The date matches,
    the number of does not match with the order data from the API.
    There has been a previously existing job match.
    The number of touches does not need to match, but does need to be populated.  
    The date of the order data and the file data are within min_diff of each other.
    """
    sql = ("SELECT count(), a.filename "
           ", c.order_number||'_'||b.order_detail_id 'job number', "
           "a.order_datetime_utc 'file utc', a.order_datetime_pst 'file pst', "
           "c.create_date_pst 'order pst', a.order_records 'file records', "
           "a.order_file_touches 'file touches', a.user_id 'file user id', "
           "b.eddm_touches 'order touches', "
           "abs(cast((julianday(a.order_datetime_pst) - "
           "julianday(c.create_date_pst)) * 24 * 60 as INTEGER )) 'min diff' "
           ", b.quantity 'order qty'"
           "FROM ProcessingFiles a JOIN OrderDetail b ON a.user_id = b.user_id "
           "JOIN OrderRequestByDate c ON b.order_id = c.order_id "
           'WHERE "min diff" <= ? '
           "AND c.order_number||'_'||b.order_detail_id IN (SELECT substr(jobname, 1, 17) from FileHistory) "
           "AND a.filename = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (min_diff, fle,))

    ans = cursor.fetchone()
    result = (ans[0] != 0, ans)

    conn.commit()
    conn.close()

    return result


def file_to_order_soft_match(fle, gblv, min_diff=120):
    """
    Returns true if there is a soft match.  The date in the matches,
    the number of does not match with the order data from the API.
    There can not be a previously existing job match.
    The number of touches does not need to match, but does need to be populated.  
    The date of the order data and the file data are within min_diff of each other.
    """
    sql = ("SELECT count(), a.filename "
           ", c.order_number||'_'||b.order_detail_id 'job number', "
           "a.order_datetime_utc 'file utc', a.order_datetime_pst 'file pst', "
           "c.create_date_pst 'order pst', a.order_records 'file records', "
           "a.order_file_touches 'file touches', a.user_id 'file user id', "
           "b.eddm_touches 'order touches', "
           "abs(cast((julianday(a.order_datetime_pst) - "
           "julianday(c.create_date_pst)) * 24 * 60 as INTEGER )) 'min diff' "
           ", b.quantity 'order qty'"
           "FROM ProcessingFiles a JOIN OrderDetail b ON a.user_id = b.user_id "
           "JOIN OrderRequestByDate c ON b.order_id = c.order_id "
           'WHERE "min diff" <= ? '
           "AND c.order_number||'_'||b.order_detail_id NOT IN (SELECT substr(jobname, 1, 17) from FileHistory) "
           "AND a.filename = ?;")

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()
    cursor.execute(sql, (min_diff, fle,))

    ans = cursor.fetchone()
    result = (ans[0] != 0, ans)

    conn.commit()
    conn.close()

    return result


def initialize_databases(gblv):

    conn = sqlite3.connect(gblv.db_name)
    cursor = conn.cursor()

    # comment out for production environment
    sql = "DROP TABLE IF EXISTS `OrderRequestByDate`;"
    cursor.execute(sql)
    sql = "DROP TABLE IF EXISTS `RequestHistory`;"
    cursor.execute(sql)
    sql = "DROP TABLE IF EXISTS `OrderDetail`;"
    cursor.execute(sql)
    sql = "DROP TABLE IF EXISTS `ProcessingFiles`;"
    cursor.execute(sql)
    sql = "DROP TABLE IF EXISTS `FileHistory`;"
    cursor.execute(sql)
    sql = "DROP TABLE IF EXISTS `ProcessingFilesHistory`;"
    cursor.execute(sql)
    conn.commit()
    # 

    sql = ("CREATE TABLE IF NOT EXISTS `OrderRequestByDate` ("
           "`order_id` INT(10) NOT NULL,"
           "`fetch_date` DATETIME NULL DEFAULT NULL,"
           "`order_number` VARCHAR(25) NULL DEFAULT NULL,"
           "`token` VARCHAR(25) NULL DEFAULT NULL,"
           "`order_description` VARCHAR(50) NULL DEFAULT NULL,"
           "`create_date` DATETIME NULL DEFAULT NULL,"
           "`create_date_pst` DATETIME NULL DEFAULT NULL,"
           "`status` VARCHAR(50) NULL DEFAULT NULL,"
           "`user_id` VARCHAR(50) NULL DEFAULT NULL,"
           "`user_group_syncmode` VARCHAR(50) NULL DEFAULT NULL,"
           "`user_group_all_or_none` VARCHAR(50) NULL DEFAULT NULL,"
           "`seller_id` VARCHAR(50) NULL DEFAULT NULL,"
           "`company_id` VARCHAR(50) NULL DEFAULT NULL,"
           "`supplier` VARCHAR(100) NULL DEFAULT NULL,"
           "`billing_address_id` VARCHAR(40) NULL DEFAULT NULL,"
           "`shipping_address_id` VARCHAR(40) NULL DEFAULT NULL,"
           "`payment_method` VARCHAR(50) NULL DEFAULT NULL,"
           "`payment_method_detail` VARCHAR(50) NULL DEFAULT NULL,"
           "`credit_card` LONGTEXT NULL DEFAULT NULL,"
           "`attached_files` VARCHAR(100) NULL DEFAULT NULL,"
           "PRIMARY KEY (`order_id`));")

    cursor.execute(sql)

    sql = ("CREATE TABLE IF NOT EXISTS `RequestHistory` ("
           "`order_id` INT(10) NOT NULL,"
           "`request_date` DATETIME NULL DEFAULT NULL,"
           "PRIMARY KEY (`order_id`));")
    cursor.execute(sql)

    # table of currently processing files
    # changes to this table needs to be reflected in 
    # ProcessingFilesHistory table and NoMatchFiles
    sql = ("CREATE TABLE IF NOT EXISTS `ProcessingFiles` ("
           "`filename` VARCHAR(100) NOT NULL,"
           "`jobname` VARCHAR(100) NULL DEFAULT NULL,"
           "`order_datetime_utc` DATETIME NULL DEFAULT NULL,"
           "`order_datetime_pst` DATETIME NULL DEFAULT NULL,"
           "`order_processed_utc` DATETIME NULL DEFAULT NULL,"
           "`order_records` INT(8) NULL DEFAULT NULL,"
           "`order_file_touches` INT(1) NULL DEFAULT NULL,"
           "`marcom_records` INT(8) NULL DEFAULT NULL,"
           "`marcom_order_touches` INT(1) NULL DEFAULT NULL,"
           "`status` VARCHAR(100) NULL DEFAULT NULL,"
           "`user_id` VARCHAR(50) NULL DEFAULT NULL,"
           "PRIMARY KEY (`filename`));")
    cursor.execute(sql)

    sql = ("CREATE TABLE IF NOT EXISTS `NoMatchFiles` ("
           "`filename` VARCHAR(100) NOT NULL,"
           "`jobname` VARCHAR(100) NULL DEFAULT NULL,"
           "`order_datetime_utc` DATETIME NULL DEFAULT NULL,"
           "`order_datetime_pst` DATETIME NULL DEFAULT NULL,"
           "`order_processed_utc` DATETIME NULL DEFAULT NULL,"
           "`order_records` INT(8) NULL DEFAULT NULL,"
           "`order_file_touches` INT(1) NULL DEFAULT NULL,"
           "`marcom_records` INT(8) NULL DEFAULT NULL,"
           "`marcom_order_touches` INT(1) NULL DEFAULT NULL,"
           "`status` VARCHAR(100) NULL DEFAULT NULL,"
           "`user_id` VARCHAR(50) NULL DEFAULT NULL,"
           "PRIMARY KEY (`filename`));")
    cursor.execute(sql)

    # table of history of processing files
    sql = ("CREATE TABLE IF NOT EXISTS `ProcessingFilesHistory` ("
           "`filename` VARCHAR(100) NOT NULL,"
           "`jobname` VARCHAR(100) NULL DEFAULT NULL,"
           "`order_datetime_utc` DATETIME NULL DEFAULT NULL,"
           "`order_datetime_pst` DATETIME NULL DEFAULT NULL,"
           "`order_processed_utc` DATETIME NULL DEFAULT NULL,"
           "`order_records` INT(8) NULL DEFAULT NULL,"
           "`order_file_touches` INT(1) NULL DEFAULT NULL,"
           "`marcom_records` INT(8) NULL DEFAULT NULL,"
           "`marcom_order_touches` INT(1) NULL DEFAULT NULL,"
           "`status` VARCHAR(100) NULL DEFAULT NULL,"
           "`user_id` VARCHAR(50) NULL DEFAULT NULL,"
           "PRIMARY KEY (`filename`));")
    cursor.execute(sql)

    # table of historical file data
    sql = ("CREATE TABLE IF NOT EXISTS `FileHistory` ("
           "`filename` VARCHAR(100) NOT NULL,"
           "`jobname` VARCHAR(100) NOT NULL,"
           "`processing_date` DATETIME NULL DEFAULT NULL,"
           "`order_records` INT(8) NULL DEFAULT NULL,"
           "`total_touches` INT(1) NULL DEFAULT NULL,"
           "`touch` INT(1) NULL DEFAULT NULL,"
           "`mailing_date` DATETIME NULL DEFAULT NULL,"
           "`user_id` VARCHAR(50) NULL DEFAULT NULL,"
           "PRIMARY KEY (`filename`));")
    cursor.execute(sql)

    sql = ("CREATE TABLE IF NOT EXISTS `OrderDetail` ("
           "`eddm_touches` INT(1) NULL,"
           "`file_match` VARCHAR(100) NULL DEFAULT NULL,"
           "`order_id` INT(11) NOT NULL,"
           "`order_detail_id` INT(11) NOT NULL,"
           "`order_type` VARCHAR(50) NULL DEFAULT NULL,"
           "`user_id` VARCHAR(50) NULL DEFAULT NULL,"
           "`req_user` VARCHAR(50) NULL DEFAULT NULL,"
           "`product_id` VARCHAR(30) NULL DEFAULT NULL,"
           "`product_name` VARCHAR(200) NULL DEFAULT NULL,"
           "`product_description` VARCHAR(100) NULL DEFAULT NULL,"
           "`sku_id` INT(11) NULL DEFAULT NULL,"
           "`sku_name` VARCHAR(200) NULL DEFAULT NULL,"
           "`sku_description` VARCHAR(250) NULL DEFAULT NULL,"
           "`quantity` INT(10) NULL DEFAULT NULL,"
           "`template_fields` LONGTEXT NULL DEFAULT NULL,"
           "`quantity_shipped` INT(10) NULL DEFAULT NULL,"
           "`price_customer` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`price_seller` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`price_shipping` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`price_unit` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`price_customer_discount` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`price_seller_misc` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`price_seller_store_discount` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`price_seller_shipping` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`price_customer_store_discount` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`price_postage` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`price_customer_misc` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_customer_sales` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_direct_acct_sales` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_city` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_county` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_state` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_district` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_city_freight` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_county_freight` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_state_freight` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_district_freight` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_total_freight` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_taxable_sales` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_exempt_sales` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_non_taxable` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`tax_city_name` VARCHAR(50) NULL DEFAULT NULL,"
           "`tax_county_name` VARCHAR(50) NULL DEFAULT NULL,"
           "`tax_state_name` VARCHAR(30) NULL DEFAULT NULL,"
           "`tax_zip` VARCHAR(10) NULL DEFAULT NULL,"
           "`department_id` VARCHAR(30) NULL DEFAULT NULL,"
           "`department_name` VARCHAR(30) NULL DEFAULT NULL,"
           "`department_number` VARCHAR(30) NULL DEFAULT NULL,"
           "`supplier_work_order_id` INT(10) NULL DEFAULT NULL,"
           "`supplier_work_order_name` VARCHAR(30) NULL DEFAULT NULL,"
           "`supplier_id` VARCHAR(30) NULL DEFAULT NULL,"
           "`shipping_date` DATETIME NULL DEFAULT NULL,"
           "`shipping_date_shipped` DATETIME NULL DEFAULT NULL,"
           "`shipping_method` VARCHAR(50) NULL DEFAULT NULL,"
           "`shipping_instructions` VARCHAR(200) NULL DEFAULT NULL,"
           "`shipping_address_id` VARCHAR(40) NULL DEFAULT NULL,"
           "`shipping_tracking` VARCHAR(20) NULL DEFAULT NULL,"
           "`shipping_tax` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`postage_cost` DECIMAL(13,2) NULL DEFAULT NULL,"
           "`postage_method` VARCHAR(50) NULL DEFAULT NULL,"
           "`client_status_value` VARCHAR(50) NULL DEFAULT NULL,"
           "`client_status_date` DATETIME NULL DEFAULT NULL,"
           "`seller_status_value` VARCHAR(50) NULL DEFAULT NULL,"
           "`seller_status_date` DATETIME NULL DEFAULT NULL,"
           "`supplier_status_value` VARCHAR(50) NULL DEFAULT NULL,"
           "`supplier_status_date` DATETIME NULL DEFAULT NULL,"
           "`credit_card_settlement` LONGTEXT NULL DEFAULT NULL,"
           "`kit` LONGTEXT NULL DEFAULT NULL,"
           "`order_order_id` VARCHAR(30) NULL DEFAULT NULL,"
           "`order_order_number` VARCHAR(30) NULL DEFAULT NULL,"
           "`client_po` VARCHAR(30) NULL DEFAULT NULL,"
           "`custom_work_order_fields` VARCHAR(30) NULL DEFAULT NULL,"
           "`sales_work_order_id` INT(10) NULL DEFAULT NULL,"
           "`product_type` VARCHAR(50) NULL DEFAULT NULL,"
           "`list_vendor` VARCHAR(50) NULL DEFAULT NULL,"
           "`finishing_options` LONGTEXT NULL DEFAULT NULL,"
           "`coupons` VARCHAR(50) NULL DEFAULT NULL,"
           "`attached_files` LONGTEXT NULL DEFAULT NULL,"
           "`uploaded_files` LONGTEXT NULL DEFAULT NULL,"
           "`sku_inventory_settings` VARCHAR(50) NULL DEFAULT NULL,"
           "`imposed_using_default_impo` VARCHAR(50) NULL DEFAULT NULL,"
           "`pagecount` INT(10) NULL DEFAULT NULL,"
           "`catalog_tree_node_id` VARCHAR(50) NULL DEFAULT NULL,"
           "`job_direct_options` VARCHAR(50) NULL DEFAULT NULL,"
           "`impersonator_fields` LONGTEXT NULL DEFAULT NULL,"
           "`requisition_status` VARCHAR(50) NULL DEFAULT NULL,"
           "`approver_user` VARCHAR(50) NULL DEFAULT NULL,"
           "`explanation` VARCHAR(50) NULL DEFAULT NULL,"
           "`job_direct_settings` VARCHAR(50) NULL DEFAULT NULL,"
           "PRIMARY KEY (`order_id`, `order_detail_id`));")
    cursor.execute(sql)

    conn.commit()
    conn.close()


def order_request_by_date(date_start, date_end, gbl, token, database=''):
    # create one settings object here so we're not creating a new instance
    # of the GlobalVar class over and over again
    request_datetime = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")

    # Make a sql connection
    conn = sqlite3.connect(gbl.db_name)
    cursor = conn.cursor()

    # get a set of all the order_id's we run before, so we can skip if already done
    history = get_request_history(cursor)

    client = zeep.Client(gbl.order_url_wsdl)
    # print(client.namespaces)

    elem = client.get_element('ns1:OrderRequestByDate')
    arg = elem(PartnerCredentials=token,
               DateRange={'Start': date_start, 'End': date_end})

    print("Initializing OrderRequestByDate API connection")
    # creates python dict
    response = (client.service.GetOrdersByDate(arg))
    print("Returning API response")

    # Initialize a commit counter
    commit_cnt = 0
    # Initialize processing date
    process_date = ''
    # Let's keep track of all the processing dates, we'll use this later to
    # update all the FedEx tables in web_api_transactions.web_request_by_date()
    process_date_set = set()

    # Loop through all the first level elements in each response record
    try:
        for n, elem in enumerate(response.GetOrdersResponse.Orders.Order):
            rec = Record()
            rec.init_values(elem, gbl.token_names[gbl.environment], request_datetime)

            user = User()
            user.init_values(elem)

            seller = Seller()
            seller.init_values(elem)

            company = Company()
            company.init_values(elem)

            bill_addr = BillAddr()
            bill_addr.init_values(elem)

            ship_addr = ShipAddr()
            ship_addr.init_values(elem)

            sales_work = SalesWork()
            order_detail_users = OrderDetailUsers()
            order_detail = OrderDetail()
            supplier = Supplier()

            for swo in elem['SalesWorkOrders']['SalesWorkOrder']:
                sales_work.append_to_group(rec['order_id'], swo)

            # the WebUser table is populated from two sources:
            # OrderRequestByDate.OrderDetails.OrderDetail and the OrderRequestByDate objects
            for od in elem['OrderDetails']['OrderDetail']:
                order_detail_users.append_to_group(od, elem)
                order_detail.append_to_group(od, rec['order_id'])
                supplier.append_to_group(od)

            # print the process date
            if datetime.datetime.strftime(rec['create_date'], '%Y-%m-%d') != process_date:
                process_date = datetime.datetime.strftime(rec['create_date'], '%Y-%m-%d')
                process_date_set.add(process_date)
                print('Processing Orders for: {0}'.format(process_date))
                # print(rec['order_id'], rec['create_date'])

            # """Insert SQLite update functions here"""
            if rec['order_id'] not in history:
                print("Updating {0} order id: {1}".format(gbl.token_names[gbl.environment], rec['order_id']))

                replace_into_table(rec, 'OrderRequestByDate', conn)
                # replace_into_table(company, 'Company', conn)
                replace_into_table(order_detail.groups, 'OrderDetail', conn)
                # replace_into_table(sales_work.groups, 'SalesWorkOrder', conn)
                # replace_into_table(supplier.groups, 'Supplier', conn)
                # replace_into_table(bill_addr, 'BillingAddress', conn)
                # replace_into_table(ship_addr, 'ShippingAddress', conn)

                # replace_into_webuser(user, conn)
                # update_webuser(order_detail_users.groups, conn)

                # replace_into_table(seller, 'Seller', conn)
                replace_into_table({'order_id': rec['order_id'],
                                    'request_date': request_datetime}, 'RequestHistory', conn)

                # commit every 10th record
                if commit_cnt == 9:
                    conn.commit()
                    commit_cnt = 0
                else:
                    commit_cnt += 1

            else:
                print("Skipping order id: {}".format(rec['order_id']))

        conn.commit()
        conn.close()
        return process_date_set

    except AttributeError:
        conn.close()
        print("No API response for chosen data range")
        return process_date_set


def print_dict(d):
    """
    Sends a readable key --> value to the console
    :param d: dictionary or list of dictionaries
    """
    if isinstance(d, (list,)):
        for entry in d:
            for key, value in entry.items():
                print("{0} --> {1}".format(key, value))

    if isinstance(d, (dict,)):
        for key, value in d.items():
            print("{0} --> {1}".format(key, value))


def replace_into_webuser(record_obj, conn):
    """
    Inserts into WebUser table.  For use with User class object only.
    This function does not include the generic_user_fields.  Those fields
    are updated with the update_webuser function, using the OrderDetailUsers class object.
    """
    try:
        cursor = conn.cursor()
        placeholders = (', '.join(['?'] * len(record_obj)))
        fields = ', '.join(record_obj.keys())
        sql = "REPLACE INTO WebUser ({fields}) VALUES ({placeholders});".format(fields=fields,
                                                                                placeholders=placeholders)
        cursor.execute(sql, list(record_obj.values()))

    except sqlite3.connector.Error as err:
        print("Error: {}".format(err))


def update_webuser(record_obj, conn):
    """
    Updates WebUser table with an OrderDetailUsers class object.  OrderDetailUsers objects
    are defined at the ORDER DETAIL level.  This will update the fields: 'fname', 'lname',
    'login_id', 'email', 'generic_user_fields' to the fields at the ORDER DETAIL level.
    """
    try:
        cursor = conn.cursor()

        for entry in record_obj:
            for key in ['fname', 'lname', 'login_id', 'email', 'generic_user_fields', 'update_date']:
                sql = "UPDATE WebUser SET {0} = %s WHERE user_id = %s;".format(key)
                cursor.execute(sql, (entry[key], entry['user_id']))

    except sqlite3.connector.Error as err:
        print("Error: {}".format(err))


def insert_into_table(record_obj, table, conn):
    """
    This should be used for inserting with no replacement.
    Possible PRIMARY KEY conflicts!
    :param record_obj: object to be used to fill table 'table'
    :param table: table to insert records into
    :param conn: current MariaDB connection
    """
    try:
        cursor = conn.cursor()

        if isinstance(record_obj, (dict,)):
            placeholders = (', '.join(['?'] * len(record_obj)))
            fields = ', '.join(record_obj.keys())
            sql = ("INSERT INTO {table} ({fields}) VALUES ({placeholders});".format(fields=fields,
                                                                                    placeholders=placeholders,
                                                                                    table=table))
            cursor.execute(sql, list(record_obj.values()))

        if isinstance(record_obj, (list,)):
            for entry in record_obj:
                placeholders = (', '.join(['?'] * len(entry)))
                fields = ', '.join(entry.keys())
                sql = ("INSERT INTO {table} ({fields}) VALUES ({placeholders});".format(fields=fields,
                                                                                        placeholders=placeholders,
                                                                                        table=table))
                cursor.execute(sql, list(entry.values()))

    except sqlite3.connector.Error as err:
        print("Error: {}".format(err))


def replace_into_table(record_obj, table, conn):
    """
    This should be used for inserting WITH replacement
    If a record already exists with the same primary index,
    the old record will be deleted and the new record will be added

    :param record_obj: object to be used to fill table 'table'
    :param table: table to insert records into
    :param conn: current MariaDB connection
    """
    try:
        cursor = conn.cursor()

        if isinstance(record_obj, (dict,)):
            placeholders = (', '.join(['?'] * len(record_obj)))
            fields = ', '.join(record_obj.keys())
            sql = ("REPLACE INTO {table} ({fields}) VALUES ({placeholders});".format(fields=fields,
                                                                                     placeholders=placeholders,
                                                                                     table=table))
            cursor.execute(sql, list(record_obj.values()))

        if isinstance(record_obj, (list,)):
            for entry in record_obj:
                placeholders = (', '.join(['?'] * len(entry)))
                fields = ', '.join(entry.keys())
                sql = ("REPLACE INTO {table} ({fields}) VALUES ({placeholders});".format(fields=fields,
                                                                                         placeholders=placeholders,
                                                                                         table=table))
                cursor.execute(sql, list(entry.values()))

    except sqlite3.connector.Error as err:
        print("Error: {}".format(err))


def get_request_history(cursor):
    """
    :return: historical list of api request order_id
    """
    results = set()
    try:
        sql = "SELECT order_id FROM RequestHistory;"
        cursor.execute(sql)
        for result in cursor:
            results.add(str(result[0]))

        return results

    except sqlite3.Error as err:
        print("Error: {}".format(err))


if __name__ == '__main__':
    pass
