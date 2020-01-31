import dbf
import csv
# import requests
# import xml.etree.ElementTree as ET
import os
import math
import shutil
import datetime
import configparser
import settings
import get_order_by_date
import fpdf
import time
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

# TODO Clean Up NoOrderMatch table
""" somewhere here there's an error where records in the NoOrderMatch table
    are not being removed in a condition where there are no previously unmatched orders
    to check.  Need to create a step to remove records from the NoOrderMatch table
    after a resolution is processed for that record
"""
"""
This script will process FB EDDM lists downloaded from eddm order portal
"""


def create_database(eddm_order, fle_path, db_name, order_file, copy_to_accuzip=True):
    """
    Writes the eddm job db file used by accuzip
    Saves a copy in the the job job folder, and saves a copy to the accuzip folder

    :param eddm_order: eddm order object
    :param fle_path: path to save the files to
    :param db_name: name of new db file
    :param order_file: name of the original order file
    :param copy_to_accuzip:
    :return:
    """
    full_newdb_path = os.path.join(fle_path, db_name)

    db = dbf.Table("{0}".format(full_newdb_path), ('FIRST C(25); ADDRESS C(1); CITY C(28); '
                                                   'ST C(2); ZIP C(10); CRRT C(4); '
                                                   'WALKSEQ_ C(7); STATUS_ C(1); '
                                                   'BARCODE C(14); X C(1)')
                   )

    db_counts = dbf.Table("{0} CRRT Counts".format(full_newdb_path), 'ZIP C(6); CRRT C(5); RES C(6); POS C(5)')

    db.open(mode=dbf.READ_WRITE)
    db_counts.open(mode=dbf.READ_WRITE)

    with open(os.path.join(gblv.current_dat_folder, order_file), 'r') as routes:
        csvr = csv.DictReader(routes, eddm_order.dat_header, delimiter='\t')
        next(csvr)
        for rec in csvr:
            addressee = eddm_order.record_addressee(rec['RouteID'])
            repeats = int(rec['Quantity'])
            db_counts.append((rec['ZipCode'], rec['RouteID'],
                              str(rec['Quantity']).zfill(5),
                              str(rec['POS']).zfill(5)), )

            for n in range(0, repeats):
                db.append((addressee, '',
                           rec['City'],
                           rec['State'],
                           rec['ZipCode'],
                           rec['RouteID'], '0000001', 'N',
                           '/{zip}{ckd}/'.format(zip=rec['ZipCode'], ckd=zip_ckd(rec['ZipCode'])),
                           ''), )

    db.close()
    db_counts.close()

    if copy_to_accuzip:
        gblv.print_log("\tmoving to accuzip folder: {}".format(db_name))
        shutil.copy2(os.path.join(fle_path, "{}.dbf".format(db_name)),
                     os.path.join(gblv.accuzip_path, "{}.dbf".format(db_name)))


def write_azzuzip_files(eddm_order, fle_path, fle, match_search, copy_to_accuzip=True):

    # If one touch, make one file
    if eddm_order.order_touches == 1:
        insert_values = {'filename': fle, 'jobname': match_search[2],
                         'processing_date': datetime.datetime.now(),
                         'order_records': eddm_order.file_qty,
                         'total_touches': eddm_order.order_touches,
                         'touch': 1, 'mailing_date': eddm_order.touch_1_maildate,
                         'user_id': match_search[8]}

        # Write eddm order db file, copy to accuzip folder
        create_database(eddm_order, fle_path, insert_values['jobname'], fle, copy_to_accuzip)
        # Create pdf tags, save to job directory
        create_job_tags(fle_path, **insert_values)
        # Insert order into FileHistory table
        get_order_by_date.update_file_history_table(gblv, **insert_values)
        # Update ini file in accuzip folder
        write_ini(insert_values['jobname'], insert_values['mailing_date'])

    # If two touches, make two files
    if eddm_order.order_touches == 2:
        for i, t in enumerate(['_1', '_2'], 1):
            insert_values = {'filename': "{0}{1}.dat".format(fle[:-4], t),
                             'jobname': "{0}_{1}".format(match_search[2], i),
                             'processing_date': datetime.datetime.now(),
                             'order_records': eddm_order.file_qty,
                             'total_touches': eddm_order.order_touches,
                             'touch': i,
                             'mailing_date': {1: eddm_order.touch_1_maildate, 2: eddm_order.touch_2_maildate}[i],
                             'user_id': match_search[8]}

            # Write eddm order db file, copy to accuzip folder
            create_database(eddm_order, fle_path, insert_values['jobname'], fle, copy_to_accuzip)
            # Create pdf tags, save to job directory
            create_job_tags(fle_path, **insert_values)
            # Insert order into FileHistory table
            get_order_by_date.update_file_history_table(gblv, **insert_values)
            # Update ini file in accuzip folder
            write_ini(insert_values['jobname'], insert_values['mailing_date'])


def process_48_hour_dat(fle):
    eddm_order = settings.EDDMOrder()
    eddm_order.set_mailing_residential(True)
    eddm_order.set_touch_1_maildate(fle[-18:-4])
    eddm_order.set_touch_2_maildate(fle[-18:-4])

    # only used in the create_database function
    gblv.current_dat_folder = gblv.no_match_orders_path

    # get number of touches in the file
    with open(os.path.join(gblv.no_match_orders_path, fle), 'r') as routes:
        csvr = csv.DictReader(routes, eddm_order.dat_header, delimiter='\t')
        next(csvr)
        running_cnt = 0
        for rec in csvr:
            eddm_order.file_touches = int(rec['NumberOfTouches'])
            eddm_order.session_id = rec['SessionID']
            running_cnt += int(rec['Quantity'])

        eddm_order.file_qty = running_cnt

    get_order_by_date.update_no_order_file_table(fle, eddm_order, gblv)
    match_search = get_order_by_date.no_match_to_order_hard_match(fle, gblv, 2880)

    if match_search[0]:
        # Successful match, all counts match, match to downloaded order data
        gblv.print_log("\tPrevious non-match order Full Match: {}".format(fle))
        # print(match_search[1])

        # Update touches to touch count in downloaded order data
        eddm_order.order_touches = match_search[1][9]
        eddm_order.order_qty = match_search[1][11]
        eddm_order.jobname = match_search[1][2]

        # Log any non-matches
        if match_search[1][7] != match_search[1][9]:
            eddm_order.processing_messages['touch_match'] = False
            update_touches_for_non_match(gblv.no_match_orders_path, fle, eddm_order)

        if match_search[1][6] != match_search[1][11]:
            eddm_order.processing_messages['count_match'] = False

        process_path = os.path.join(gblv.save_orders_path, match_search[1][2])

        create_directory_path(process_path)
        # Copy original file into new directory, in 'original' folder
        move_file_to_new_folder(gblv.no_match_orders_path,
                                os.path.join(process_path, 'original'),
                                fle)

        # Write accuzip dbf files for this job, and save copy to accuzip folder
        write_azzuzip_files(eddm_order, process_path, fle, match_search[1])
        # update processing files table, set processing date
        get_order_by_date.extended_update_no_match_table(gblv, fle, eddm_order)
        get_order_by_date.status_update_processing_no_match_table(gblv, fle, "Previous non-match: "
                                                                             "Hard match, order processed")
        # Copy file to complete_processing_files path
        move_file_to_new_folder(gblv.no_match_orders_path,
                                gblv.complete_processing_path, fle, delete_original=gblv.delete_original_files)

    elif get_order_by_date.no_match_to_order_soft_match(fle, gblv, 2880)[0]:
        # Soft match, mail counts don't match, touch count may not match
        match_search = get_order_by_date.no_match_to_order_soft_match(fle, gblv, 2880)

        gblv.print_log("Previous non-match order Soft Match: {}".format(fle))
        # print(match_search[1])

        # Update touches to touch count in downloaded order data
        eddm_order.order_touches = match_search[1][9]
        eddm_order.order_qty = match_search[1][11]
        eddm_order.jobname = match_search[1][2]

        # Log any non-matches
        if match_search[1][7] != match_search[1][9]:
            eddm_order.processing_messages['touch_match'] = False
            update_touches_for_non_match(gblv.no_match_orders_path, fle, eddm_order)

        if match_search[1][6] != match_search[1][11]:
            eddm_order.processing_messages['count_match'] = False

        # process_path = os.path.join(gblv.no_match_orders_path, match_search[1][2])
        process_path = os.path.join(gblv.hold_orders_path, match_search[1][2])

        create_directory_path(process_path)
        # Copy original file into new directory, in 'original' folder
        move_file_to_new_folder(gblv.no_match_orders_path,
                                os.path.join(process_path, 'original'),
                                fle)

        # Write accuzip dbf files for this job, and save copy to accuzip folder
        write_azzuzip_files(eddm_order, process_path, fle, match_search[1], False)
        # update processing files table, set processing date
        get_order_by_date.extended_update_no_match_table(gblv, fle, eddm_order)
        get_order_by_date.status_update_processing_no_match_table(gblv, fle, "Previous non-match order: "
                                                                             "Soft match, moved to hold")
        # Copy file to complete_processing_files path
        move_file_to_new_folder(gblv.no_match_orders_path,
                                gblv.complete_processing_path, fle, delete_original=gblv.delete_original_files)

    elif get_order_by_date.file_to_order_previous_match(fle, gblv, 2880)[0]:
        # Soft match, mail counts don't match, touch count may not match, matches to previous order
        gblv.print_log("Previous non-match order: Match to previous order: {}".format(fle))
        get_order_by_date.status_update_processing_file_table(gblv, fle, "Previous non-match order: "
                                                                         "Soft match to previous job, moved to hold")
        create_directory_path(gblv.duplicate_orders_path)
        # Copy file to complete_processing_files path
        move_file_to_new_folder(gblv.no_match_orders_path, gblv.complete_processing_path, fle)
        # Copy file to no duplicates folder, and delete from downloaded orders path
        move_file_to_new_folder(gblv.no_match_orders_path,
                                gblv.duplicate_orders_path,
                                fle, delete_original=gblv.delete_original_files)

    else:
        # No match, move to error
        gblv.print_log("No match to Marcom order: {}".format(fle))
        get_order_by_date.status_update_processing_no_match_table(gblv, fle, "NO MATCH TO MARCOM ORDER")


def process_dat(fle):
    eddm_order = settings.EDDMOrder()
    eddm_order.set_mailing_residential(True)
    eddm_order.set_touch_1_maildate(fle[-18:-4])
    eddm_order.set_touch_2_maildate(fle[-18:-4])

    # only used in the create_database function
    gblv.current_dat_folder = gblv.downloaded_orders_path

    # get number of touches in the file
    with open(os.path.join(gblv.downloaded_orders_path, fle), 'r') as routes:
        csvr = csv.DictReader(routes, eddm_order.dat_header, delimiter='\t')
        next(csvr)
        running_cnt = 0
        for rec in csvr:
            eddm_order.file_touches = int(rec['NumberOfTouches'])
            eddm_order.session_id = rec['SessionID']
            running_cnt += int(rec['Quantity'])

        eddm_order.file_qty = running_cnt

    get_order_by_date.update_processing_file_table(fle, eddm_order, gblv)
    match_search = get_order_by_date.file_to_order_hard_match(fle, gblv, 120)

    if match_search[0]:
        # Successful match, all counts match, match to downloaded order data
        gblv.print_log("Full Match: {}".format(fle))
        # print(match_search[1])

        # Update touches to touch count in downloaded order data
        eddm_order.order_touches = match_search[1][9]
        eddm_order.order_qty = match_search[1][11]
        eddm_order.jobname = match_search[1][2]

        # Log any non-matches
        if match_search[1][7] != match_search[1][9]:
            eddm_order.processing_messages['touch_match'] = False
            update_touches_for_non_match(gblv.downloaded_orders_path, fle, eddm_order)

        if match_search[1][6] != match_search[1][11]:
            eddm_order.processing_messages['count_match'] = False

        process_path = os.path.join(gblv.save_orders_path, match_search[1][2])

        create_directory_path(process_path)
        # Copy original file into new directory, in 'original' folder
        move_file_to_new_folder(gblv.downloaded_orders_path,
                                os.path.join(process_path, 'original'),
                                fle)

        # Write accuzip dbf files for this job, and save copy to accuzip folder
        write_azzuzip_files(eddm_order, process_path, fle, match_search[1])
        # update processing files table, set processing date
        get_order_by_date.extended_update_processing_file_table(gblv, fle, eddm_order)
        get_order_by_date.status_update_processing_file_table(gblv, fle, "Hard match, order processed")
        # Copy file to complete_processing_files path
        move_file_to_new_folder(gblv.downloaded_orders_path,
                                gblv.complete_processing_path, fle, delete_original=gblv.delete_original_files)

    elif get_order_by_date.file_to_order_hard_previous_match(fle, gblv, 120)[0]:
        # Hard match, matches to previous order
        gblv.print_log("Hard match to previous order: {}".format(fle))
        get_order_by_date.status_update_processing_file_table(gblv, fle,
                                                              "Hard match to previous job, moved to duplicate")
        create_directory_path(gblv.duplicate_orders_path)
        # Copy file to complete_processing_files path
        move_file_to_new_folder(gblv.downloaded_orders_path, gblv.complete_processing_path, fle)
        # Copy file to no duplicates folder, and delete from downloaded orders path
        move_file_to_new_folder(gblv.downloaded_orders_path,
                                os.path.join(gblv.duplicate_orders_path),
                                fle, delete_original=gblv.delete_original_files)

    elif get_order_by_date.file_to_order_soft_match(fle, gblv, 120)[0]:
        # Soft match, mail counts don't match, touch count may not match
        match_search = get_order_by_date.file_to_order_soft_match(fle, gblv, 120)

        gblv.print_log("Soft Match: {}".format(fle))
        # print(match_search[1])

        # Update touches to touch count in downloaded order data
        eddm_order.order_touches = match_search[1][9]
        eddm_order.order_qty = match_search[1][11]
        eddm_order.jobname = match_search[1][2]

        # Log any non-matches
        if match_search[1][7] != match_search[1][9]:
            eddm_order.processing_messages['touch_match'] = False
            update_touches_for_non_match(gblv.downloaded_orders_path, fle, eddm_order)

        if match_search[1][6] != match_search[1][11]:
            eddm_order.processing_messages['count_match'] = False

        process_path = os.path.join(gblv.hold_orders_path, match_search[1][2])

        create_directory_path(process_path)
        # Copy original file into new directory, in 'original' folder
        move_file_to_new_folder(gblv.downloaded_orders_path,
                                os.path.join(process_path, 'original'),
                                fle)

        # Write accuzip dbf files for this job, and save copy to accuzip folder
        write_azzuzip_files(eddm_order, process_path, fle, match_search[1], False)
        # update processing files table, set processing date
        get_order_by_date.extended_update_processing_file_table(gblv, fle, eddm_order)
        get_order_by_date.status_update_processing_file_table(gblv, fle, "Soft match, moved to hold")
        # Copy file to complete_processing_files path
        move_file_to_new_folder(gblv.downloaded_orders_path,
                                gblv.complete_processing_path, fle, delete_original=gblv.delete_original_files)

    elif get_order_by_date.file_to_order_previous_match(fle, gblv, 120)[0]:
        # Soft match, mail counts don't match, touch count may not match, matches to previous order
        gblv.print_log("Match to previous order: {}".format(fle))
        get_order_by_date.status_update_processing_file_table(gblv, fle, "Soft match to previous job, moved to hold")
        create_directory_path(gblv.duplicate_orders_path)
        # Copy file to complete_processing_files path
        move_file_to_new_folder(gblv.downloaded_orders_path, gblv.complete_processing_path, fle)
        # Copy file to duplicates folder, and delete from downloaded orders path
        move_file_to_new_folder(gblv.downloaded_orders_path,
                                gblv.duplicate_orders_path,
                                fle, delete_original=gblv.delete_original_files)

    else:
        # No match, move to error
        gblv.print_log("No match to Marcom order: {}".format(fle))
        get_order_by_date.status_update_processing_file_table(gblv, fle, "NO MATCH TO MARCOM ORDER")
        create_directory_path(gblv.no_match_orders_path)
        # Copy file to complete_processing_files path
        move_file_to_new_folder(gblv.downloaded_orders_path, gblv.complete_processing_path, fle)
        # Copy file to no match folder, and delete from downloaded orders path
        move_file_to_new_folder(gblv.downloaded_orders_path,
                                gblv.no_match_orders_path,
                                fle, delete_original=gblv.delete_original_files)


def zip_ckd(zipcode):
    val = sum_digits(zipcode)
    return int(((math.ceil(val / 10.0)) * 10) - val)


def create_job_tags(fle_path, **val):

    if val['total_touches'] > 1:
        if val['touch'] == 2:
            pdf_name = "{} JOB TAGS.pdf".format(val['jobname'])
            pdf = fpdf.FPDF('L', 'in', 'Letter')
            pdf.add_page()
            pdf.set_margins(.25, 0, 0)
            pdf.set_font('Arial', 'B', 60)
            pdf.cell(10.5, 1.25, val['jobname'], 0, 0, 'C')
            pdf.set_font('Arial', 'B', 70)
            pdf.set_y(pdf.get_y() + 1.25)
            pdf.cell(10.5, 1.25, "Mail Date: {}".format(datetime.datetime.strftime(val['mailing_date'],
                                                                                   "%Y/%m/%d")), 0, 0,
                     'C')
            pdf.set_y(pdf.get_y() + 1.25)
            pdf.cell(10.5, 1.25, "Total Qty: {:,}".format(val['order_records']), 0, 0, 'C')
            pdf.set_y(pdf.get_y() + 1.25)
            pdf.cell(10.5, 1.25, "Touch {} of 2".format(val['touch']), 0, 0, 'C')

            pdf.set_y(pdf.get_y() + 1.4)
            pdf.set_font('Arial', 'B', 60)
            pdf.cell(10.5, 1.25, "WAIT FOR APPROVAL".format(val['touch']), 0, 0, 'C')
            pdf.set_y(pdf.get_y() + .75)
            pdf.cell(10.5, 1.25, "BEFORE MAILING".format(val['touch']), 0, 0, 'C')
        else:
            pdf_name = "{} JOB TAGS.pdf".format(val['jobname'])
            pdf = fpdf.FPDF('L', 'in', 'Letter')
            pdf.add_page()
            pdf.set_margins(.25, 0, 0)
            pdf.set_font('Arial', 'B', 60)
            pdf.cell(10.5, 1.5, val['jobname'], 0, 0, 'C')
            pdf.set_font('Arial', 'B', 70)
            pdf.set_y(pdf.get_y() + 2)
            pdf.cell(10.5, 1.5, "Mail Date: {}".format(datetime.datetime.strftime(val['mailing_date'],
                                                                                  "%Y/%m/%d")), 0, 0, 'C')
            pdf.set_y(pdf.get_y() + 2)
            pdf.cell(10.5, 1.5, "Total Qty: {:,}".format(val['order_records']), 0, 0, 'C')

            pdf.set_y(pdf.get_y() + 1.6)
            pdf.cell(10.5, 1.25, "Touch {} of 2".format(val['touch']), 0, 0, 'C')

    else:
        pdf_name = "{} JOB TAGS.pdf".format(val['jobname'])
        pdf = fpdf.FPDF('L', 'in', 'Letter')
        pdf.add_page()
        pdf.set_margins(.25, 0, 0)
        pdf.set_font('Arial', 'B', 70)
        pdf.cell(10.5, 1.5, val['jobname'], 0, 0, 'C')
        pdf.set_y(pdf.get_y() + 2)
        pdf.cell(10.5, 1.5, "Mail Date: {}".format(datetime.datetime.strftime(val['mailing_date'], "%Y/%m/%d")), 0, 0,
                 'C')
        pdf.set_y(pdf.get_y() + 2)
        pdf.cell(10.5, 1.5, "Total Qty: {:,}".format(val['order_records']), 0, 0, 'C')

    pdf.output(os.path.join(fle_path, pdf_name), 'F')


def sum_digits(n):
    if not isinstance(n, (int,)):
        n = int(n)

    r = 0
    while n:
        r, n = r + n % 10, n // 10
    return r


def write_ini(fle, mailing_date):

    configfile = os.path.join(gblv.accuzip_path, 'mail_dates.ini')

    if not os.path.exists(configfile):
        config = configparser.ConfigParser()
        config.add_section('mailing_dates')
        with open(configfile, 'w') as c:
            config.write(c)

    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(configfile)

    config.set('mailing_dates', fle, datetime.datetime.strftime(mailing_date, "%m/%d/%Y"))

    with open(configfile, 'w') as c:
        config.write(c)


def download_web_orders(back_days):

    # year = 2019
    # month_start = 7
    # day_start = 1
    # month_end = 7
    # day_end = 8
    # date_start = (datetime.datetime.strptime("{y}-{m}-{d} 00:00:00".format(
    #               m=month_start,y=year,d=str(day_start).zfill(2)),"%Y-%m-%d %H:%M:%S"))

    # date_end = (datetime.datetime.strptime("{y}-{m}-{d} 23:59:59".format(
    #               m=month_end,y=year,d=str(day_end).zfill(2)),"%Y-%m-%d %H:%M:%S"))

    date_end = datetime.datetime.today()
    date_start = date_end - datetime.timedelta(days=back_days)

    print("Searching back {} days for new orders".format(back_days))

    gblv.print_log("Downloading orders from {} to {}".format(datetime.datetime.strftime(date_start, "%m/%d/%Y"),
                                                             datetime.datetime.strftime(date_end, "%m/%d/%Y")))

    get_order_by_date.order_request_by_date(date_start, date_end, gblv, gblv.token)
    get_order_by_date.clean_unused_order_detail(gblv)
    get_order_by_date.clean_unused_orders(gblv)


def create_directory_path(process_path, overwrite=False):
    # Creates this folder structure for the file
    if overwrite:
        if os.path.exists(process_path):
            shutil.rmtree(process_path)
            os.makedirs(process_path)
        else:
            os.makedirs(process_path)
    else:
        if not os.path.exists(process_path):
            os.makedirs(process_path)


def move_file_to_new_folder(from_path, to_path, fle, overwrite=False, delete_original=False):
    # Creates this folder structure for the file, does not overwrite by default
    # Moves fle from from_path to to_path, does not delete old file by default
    if overwrite:
        if os.path.exists(to_path):
            shutil.rmtree(to_path)
            os.mkdir(to_path)
        else:
            os.mkdir(to_path)
    else:
        if not os.path.exists(to_path):
            os.mkdir(to_path)

    shutil.copy2(os.path.join(from_path, fle),
                 os.path.join(to_path, fle))

    if delete_original:
        os.remove(os.path.join(from_path, fle))


def process_non_match(hours):
    """
    Processes previously non-matched files.  Files older than [hours] are moved into a deleted folder,
    routes will need to be released.
    Lists less than [hours] old will be run through processing again in an attempt to
    match to Marcom.
    """
    gblv.print_log("Processing previous non-match records")
    gblv.current_dat_folder = gblv.no_match_orders_path

    # First, find out if there are any orders that haven't been matched yet.
    # If there are no orders that have not been matched, immmediately delete orders over hour threshold,
    # Otherwise, look to match unmatched orders to order detail orders that havevn't been matched.

    # Create non-match object for processing
    non_match = settings.NonMatchOrders(hours)
    # Populate lists of records in no_order_match directory that are over and under hour threshold
    non_match.set_threshold_lists(gblv, to_console=True)
    # Order those lists by date, newest first
    non_match.file_over_threshold = date_ordered_file_list(non_match.file_over_threshold)
    non_match.file_under_threshold = date_ordered_file_list(non_match.file_under_threshold)

    # Figure out if any orders haven't been matched to previous data files
    if int(get_order_by_date.count_unmatched_orders_order_detail(gblv)[0][0]) > 0:
        gblv.print_log("Searching previously unmatched Marcom orders")

        # Create table of orders to process
        get_order_by_date.no_match_files_table(gblv, non_match.file_under_threshold)

        for order in non_match.file_under_threshold:
            process_48_hour_dat(order)
    else:
        gblv.print_log("\tNo unmatched Marcom orders to search")

    if non_match.file_over_threshold:
        gblv.print_log("Processing files to unlock routes")
        get_order_by_date.delete_orders_table(gblv)
        for order in non_match.file_over_threshold:
            gblv.print_log("\tUnlocking routes for {}".format(order))
            # All all records from old orders into delete_order_records table
            with open(os.path.join(gblv.no_match_orders_path, order), 'r') as o:
                csvr = csv.DictReader(o, ['AgentID', 'DateSelected', 'City', 'State',
                                          'ZipCode', 'RouteID', 'Quantity', 'POS',
                                          'NumberOfTouches', 'SessionID'], delimiter='\t')
                next(csvr)
                for line in csvr:
                    get_order_by_date.insert_into_delete_orders_table(gblv, order, line)

        # create set of session ids to unlock
        session_id = get_order_by_date.get_session_id_sqlite(gblv, 'delete_order_records')
        # iterate through sessions ids and unlock routes
        if session_id:
            get_order_by_date.delete_order_record_unlock_routes(gblv, session_id)

    # Any orders that are over [hours], move to deleted directory
    if non_match.file_over_threshold:
        gblv.print_log("Moving orders older than {} hours to deleted directory".format(hours))
        for order in non_match.file_over_threshold:
            gblv.print_log("\tMoving {} to deleted_orders".format(order))
            move_file_to_new_folder(gblv.no_match_orders_path,
                                    gblv.deleted_orders_path, order,
                                    delete_original=True)


def update_touches_for_non_match(processing_file_path, fle, eddm_order):
    """
    Updates Marcom for orders that don't have matching touches in Marcom and the .dat data
    """
    gblv.print_log("Updating touch count for {}: {}".format(fle, eddm_order.jobname))
    # Create a table of order records that need to be updated
    get_order_by_date.update_order_touches_table(gblv)

    with open(os.path.join(processing_file_path, fle), 'r') as o:
        csvr = csv.DictReader(o, eddm_order.dat_header, delimiter='\t')
        next(csvr)
        for line in csvr:
            get_order_by_date.insert_into_update_order_touches_table(gblv, fle, line)

    # get session id to update touches
    session_id = get_order_by_date.get_session_id_sqlite(gblv, 'update_touch_records')
    get_order_by_date.order_submit_update_route_touches(gblv, session_id, eddm_order.order_touches)


def date_ordered_file_list(eval_list):
    dic = {}
    for order in eval_list:
        dic[order[:-4].split("_")[1]] = order

    sorted_dic = sorted(dic.items(), key=lambda kv: datetime.datetime.strptime(kv[0], "%Y%m%d%H%M%S"), reverse=True)

    return [v for k, v in sorted_dic]


def write_tag_merge():
    """Can write a merge file for running tags through a fusion pro job"""
    tag_filename = "TAG_MERGE_{datestring}.txt".format(
            datestring=datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d_%I %M %p"))

    with open(os.path.join(gblv.downloaded_orders_path, tag_filename), 'w+') as log:
        log.write("jobname\tmailing date\tfilecount\n")
        for line in get_order_by_date.processing_files_log(gblv):
            log.write("{0}\t{1}\t{2:,}\n".format(line[1], line[8], line[3]))


def email_agent_status(days):
    port = 25
    smtp_server = gblv.email_server
    sender_email = gblv.email_user
    email_from = gblv.email_from
    receiver_email = gblv.agent_email

    pdt = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d")

    html_head = ("<html> <head> <style> td, th { border: 1px solid #dddddd; "
                 "text-align: left; padding: 8px;}</style> </head> <body> ")

    html_foot = "</body> </html><p>"
    date_from = datetime.datetime.strftime(datetime.datetime.today(), "%Y/%m/%d")
    date_to = datetime.datetime.strftime(datetime.timedelta(days=(days - 1)) + datetime.datetime.today(), "%Y/%m/%d")

    message_html = (f"<p>Agent status for jobs mailing {date_from} - {date_to}<br><p/><table width: 100%;> "
                    "<tr><th>Job Name</th><th>Mailing Date</th><th>Agent ID</th><th>Status</th>"
                    "<th>Agent Name</th></tr>")

    for line in get_order_by_date.jobs_mailing_agent_status(gblv, days):
        message_html += (f"<tr><td>{line[0]}</td><td>{line[1]}</td><td>{line[2]}</td><td>{line[4]}</td>"
                         f"<td>{line[5]}</td></tr>")

    message_html += "</table>"
    message_html += ("<p>Report run {0}<br>".format(datetime.datetime.strftime(datetime.datetime.now(),
                                                                               "%Y-%m-%d %I:%M %p")))
    message_html += ("V2FBLUSERDATA last updated: {0}</p>".format(
            get_order_by_date.v2fbluserdata_update_date(gblv)[0][0]))

    html = f"{html_head}{message_html}{html_foot}"
    subject = "Agent to job status for EDDM orders on {0}".format(datetime.datetime.strftime(datetime.datetime.now(),
                                                                                             "%Y-%m-%d"))
    text = ""
    message = MIMEMultipart("alternative")

    message["Subject"] = subject
    message["From"] = sender_email
    message["To"] = receiver_email

    message.attach(MIMEText(text, "plain"))
    message.attach(MIMEText(html, "html"))

    with smtplib.SMTP(smtp_server, port) as server:
        print("sending EDDM agent email")
        server.starttls()
        server.sendmail(email_from, message["To"].split(","),
                        message.as_string())


def job_agent_status(days):
    """
    Writes a report of jobs in the next [days] days, and the agent status of each job
    :param days:
    :return:
    """
    report_filename = "Agent_Job Status_{datestring}.txt".format(
            datestring=datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d"))

    date_from = datetime.datetime.strftime(datetime.datetime.today(), "%Y/%m/%d")
    date_to = datetime.datetime.strftime(datetime.timedelta(days=(days - 1)) + datetime.datetime.today(), "%Y/%m/%d")

    with open(os.path.join(gblv.shared_path, report_filename), 'w+') as log:
        log.write("Agent status for jobs mailing {} - {}\n\n".format(date_from, date_to))

        log.write("{:<25}{:<18}{:<12}{:<12}{:<40}\n".format("Job Name",
                                                            "Mailing Date",
                                                            "Agent ID",
                                                            "Status",
                                                            "Agent Name"))

        for line in get_order_by_date.jobs_mailing_agent_status(gblv, days):
            log.write("{:<25}{:<18}{:<12}{:<12}{:<40}\n".format(line[0], line[1], line[2], line[4], line[5]))

        log.write("\n\nReport run {}".format(datetime.datetime.strftime(datetime.datetime.now(),
                                                                        "%Y-%m-%d %I:%M %p")))

        log.write("\nV2FBLUSERDATA last updated: {}\n".format(get_order_by_date.v2fbluserdata_update_date(gblv)[0][0]))


def email_message_log():
    port = 25
    smtp_server = gblv.email_server
    sender_email = gblv.email_user
    email_from = gblv.email_from
    receiver_email = gblv.email_to

    pdt = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S")

    html_head = ("<html> <head> <style> td, th { border: 1px solid #dddddd; "
                 "text-align: left; padding: 8px;}</style> </head> <body> ")
    html_foot = "</body> </html><p>"
    part_1_html = ""
    part_2_html = ""
    part_3_html = ""
    message_html = ""

    for message in gblv.log_messages:
        message_html += f"{message}<br>"
    message_html += "</p>"

    if get_order_by_date.processing_files_log(gblv):
        part_1_html = ("<p>Summary of new files processed<br><p/><table width: 100%;> "
                       "<tr><th>File name</th><th>Job name</th><th>Order Date</th><th>Mailing date</th>"
                       "<th>File count</th><th>File Touches</th><th>Marcom Count</th>"
                       "<th>Marcom touches</th><th>Status</th></tr>")

        for line in get_order_by_date.processing_files_log(gblv):
            part_1_html += (f"<tr><td>{line[0]}</td><td>{line[1]}</td><td>{line[2]}</td><td>{line[8]}</td>"
                            f"<td>{line[3]}</td><td>{line[4]}</td><td>{line[5]}</td>"
                            f"<td>{line[6]}</td><td>{line[7]}</td></tr>")

        part_1_html += "</table>"

    else:
        part_1_html = "<p>No new files processed<br><p/>"

    if get_order_by_date.nomatch_processing_files_log(gblv):
        part_2_html = ("<p>Summary of non-match files processed<br><p/><table width: 100%;> "
                       "<tr><th>File name</th><th>Job name</th><th>Order Date</th><th>Mailing date</th>"
                       "<th>File count</th><th>File Touches</th><th>Marcom Count</th>"
                       "<th>Marcom touches</th><th>Status</th></tr>")

        for line in get_order_by_date.nomatch_processing_files_log(gblv):
            part_2_html += (f"<tr><td>{line[0]}</td><td>{line[1]}</td><td>{line[2]}</td><td>{line[8]}</td>"
                            f"<td>{line[3]}</td><td>{line[4]}</td><td>{line[5]}</td>"
                            f"<td>{line[6]}</td><td>{line[7]}</td></tr>")

        part_2_html += "</table>"

    else:
        part_2_html = "<p>No non-match files processed<br><p/>"

    if get_order_by_date.marcom_orders_unmatched(gblv):
        part_3_html = ("<p>Unmatched Marcom orders<br><p/><table width: 100%;> "
                       "<tr><th>Order Date</th><th>User ID</th><th>Order ID</th><th>Order Detail ID</th>"
                       "<th>Order Number</th><th>Qty</th></tr>")

        for line in get_order_by_date.marcom_orders_unmatched(gblv):
            part_3_html += (f"<tr><td>{line[0]}</td><td>{line[1]}</td><td>{line[2]}</td>"
                            f"<td>{line[3]}</td><td>{line[4]}</td><td>{line[5]}</td></tr>")
        part_3_html += "</table>"

    else:
        part_3_html = "<p>No unmatched Marcom orders<br><p/>"

    html = f"{html_head}{message_html}{part_1_html}{part_2_html}{part_3_html}{html_foot}"
    subject = f"FB EDDM for processing {pdt}"

    text = ""
    message = MIMEMultipart("alternative")

    message["Subject"] = subject
    message["From"] = sender_email
    message["To"] = receiver_email

    message.attach(MIMEText(text, "plain"))
    message.attach(MIMEText(html, "html"))

    with smtplib.SMTP(smtp_server, port) as server:
        print("sending EDDM log email")
        server.starttls()
        server.sendmail(email_from, message["To"].split(","),
                        message.as_string())


def write_message_log():

    log_filename = "LOG_{datestring}.txt".format(
            datestring=datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d_%H%M%S"))

    with open(os.path.join(gblv.shared_path, log_filename), 'w+') as log:
        for message in gblv.log_messages:
            log.write("{}\n".format(message))

        if get_order_by_date.processing_files_log(gblv):
            log.write("\n\nSummary of new files processed:\n\n")
            log.write("{:<28}{:<24}{:<24}{:<14}{:>10}{:>15}{:>15}"
                      "{:>17}{:>5}{:<100}\n".format("filename",
                                                    "jobname",
                                                    "order date",
                                                    "mailing date",
                                                    "file count",
                                                    "file touches",
                                                    "marcom count",
                                                    "marcom touches",
                                                    "",
                                                    "status"))

            for line in get_order_by_date.processing_files_log(gblv):
                log.write("{:<28}{:<24}{:<24}{:<14}{:>10,}{:>15,}"
                          "{:>15,}{:>17,}{:>5}{:<100}\n".format(line[0],
                                                                line[1],
                                                                line[2],
                                                                line[8],
                                                                line[3],
                                                                line[4],
                                                                line[5],
                                                                line[6],
                                                                "",
                                                                line[7]))
        else:
            log.write("\n\nNo new files processed\n")

        if get_order_by_date.nomatch_processing_files_log(gblv):
            log.write("\n\nSummary of non-match files processed:\n\n")
            log.write("{:<28}{:<24}{:<24}{:<14}{:>10}{:>15}{:>15}"
                      "{:>17}{:>5}{:<100}\n".format("filename",
                                                    "jobname",
                                                    "order date",
                                                    "mailing date",
                                                    "file count",
                                                    "file touches",
                                                    "marcom count",
                                                    "marcom touches",
                                                    "",
                                                    "status"))

            for line in get_order_by_date.nomatch_processing_files_log(gblv):
                log.write("{:<28}{:<24}{:<24}{:<14}{:>10,}{:>15,}"
                          "{:>15,}{:>17,}{:>5}{:<100}\n".format(line[0],
                                                                line[1],
                                                                line[2],
                                                                line[8],
                                                                line[3],
                                                                line[4],
                                                                line[5],
                                                                line[6],
                                                                "",
                                                                line[7]))
        else:
            log.write("\n\nNo non-match files processed\n")

        if get_order_by_date.marcom_orders_unmatched(gblv):
            log.write("\n\nUnmatched Marcom orders:\n\n")
            log.write("{:<23}{:<10}{:<12}{:<18}{:<12}{:>8}\n".format("Order Date",
                                                                     "User ID",
                                                                     "Order ID",
                                                                     "Order Detail ID",
                                                                     "Order Number",
                                                                     "Qty"))

            for line in get_order_by_date.marcom_orders_unmatched(gblv):
                log.write("{:<23}{:<10}{:<12}{:<18}{:<12}{:>8,}\n".format(line[0],
                                                                          line[1],
                                                                          line[2],
                                                                          line[3],
                                                                          line[4],
                                                                          line[5]))
        else:
            log.write("\n\nNo unmatched Marcom orders\n")


def set_up_functions():
    pass
    # For setup, don't run in production
    # get_order_by_date.initialize_databases(gblv)
    # get_order_by_date.clear_file_history_table(gblv)
    #


def move_to_working_dir():
    """
    This program doesn't have write priveleges to FTP folder where new .dat files are downloaded, this
    function will copy any files from the download folder into a working directory that do not already
    exist in the complete processing files folder (have already gone through processing).  That 
    directory will then considered the 'download' directory
    """
    ftp_directory = set([f for f in os.listdir(gblv.ftp_directory) 
                           if f[-3:].upper() == 'DAT'])

    working_folder = set([f for f in os.listdir(gblv.complete_processing_path) 
                           if f[-3:].upper() == 'DAT'])

    unprocessed_files = set(ftp_directory - working_folder)

    for fle in unprocessed_files:
        gblv.print_log("Moving {} to unprocessed_orders directory".format(fle))
        move_file_to_new_folder(gblv.ftp_directory, gblv.downloaded_orders_path, fle)


def run_processing():
    global gblv
    gblv = settings.GlobalVar()
    # Set environment to 'PRODUCTION' for production
    # gblv.set_environment('QA')
    gblv.set_environment('PRODUCTION')
    gblv.set_order_paths()
    move_to_working_dir()
    gblv.create_accuzip_dir()
    gblv.set_token_name()
    gblv.set_db_name()

    # FOR SETUP ONLY set_up_functions()

    gblv.print_log("Importing V2FBLUSERDATA")
    get_order_by_date.import_userdata(gblv)
    gblv.print_log("Import complete")
    gblv.print_log("Vacuuming database")
    get_order_by_date.vacuum_database(gblv)

    get_order_by_date.clear_processing_files_table(gblv)

    # Download orders, go back n days
    download_web_orders(int(gblv.fetch_n_days))

    # Create a list of orders
    downloaded_orders = [f for f in os.listdir(gblv.downloaded_orders_path) if f[-3:].upper() == 'DAT']
    orders = date_ordered_file_list(downloaded_orders)
    # Create table of orders to process
    get_order_by_date.processing_files_table(gblv, orders)

    for order in orders:
        process_dat(order)

    if not len(orders):
        gblv.print_log("No new files to process")

    process_non_match(48)

    get_order_by_date.append_filename_to_orderdetail(gblv)
    get_order_by_date.append_filename_to_orderdetail_48_hour(gblv)
    get_order_by_date.processing_table_to_history(gblv)

    try:
        email_message_log()
        if datetime.date.today().weekday() in (1, 3):
            email_agent_status(5)
        write_message_log()
        job_agent_status(5)
    except Exception as e:
        print(e)
        write_message_log()
        job_agent_status(5)


def force_processing(file_name, order_detail_order_id):
    """
    Forces processing of file [file_name], matches to [order_detail_order_id] == OrderDetail.order_detail_id
    Use Caution!!!
    :param file_name: name of file, must exist in downloaded orders path
    :param order_detail_order_id: order detail id in OrderDetail.order_order_detail_id
    :return:
    """
    global gblv
    gblv = settings.GlobalVar()
    # Set environment to 'PRODUCTION' for production
    # gblv.set_environment('QA')
    gblv.set_environment('PRODUCTION')
    gblv.set_order_paths()
    gblv.create_accuzip_dir()
    gblv.set_token_name()
    gblv.set_db_name()

    if not os.path.exists(os.path.join(gblv.downloaded_orders_path, file_name)):
        print("'{0}' not found in {1}".format(file_name, gblv.downloaded_orders_path))
        time.sleep(4)
        return -1

    eddm_order = settings.EDDMOrder()
    eddm_order.set_mailing_residential(True)
    eddm_order.set_touch_1_maildate(file_name[-18:-4])
    eddm_order.set_touch_2_maildate(file_name[-18:-4])

    # get number of touches in the file
    with open(os.path.join(gblv.downloaded_orders_path, file_name), 'r') as routes:
        csvr = csv.DictReader(routes, eddm_order.dat_header, delimiter='\t')
        next(csvr)
        running_cnt = 0
        for rec in csvr:
            eddm_order.file_touches = int(rec['NumberOfTouches'])
            eddm_order.session_id = rec['SessionID']
            running_cnt += int(rec['Quantity'])

        eddm_order.file_qty = running_cnt

    get_order_by_date.update_processing_file_table(file_name, eddm_order, gblv)
    match_search = get_order_by_date.file_to_order_force_match(file_name, order_detail_order_id, gblv)

    gblv.print_log("Force Match: {}".format(file_name))

    # Update touches to touch count in downloaded order data
    eddm_order.order_touches = match_search[1][9]
    eddm_order.order_qty = match_search[1][11]
    eddm_order.jobname = match_search[1][2]

    # Log any non-matches
    if match_search[1][7] != match_search[1][9]:
        eddm_order.processing_messages['touch_match'] = False

    if match_search[1][6] != match_search[1][11]:
        eddm_order.processing_messages['count_match'] = False

    # process_path = os.path.join(gblv.downloaded_orders_path, match_search[1][2])
    process_path = os.path.join(gblv.downloaded_orders_path, match_search[1][2])

    create_directory_path(process_path)
    # Copy original file into new directory, in 'original' folder
    move_file_to_new_folder(gblv.downloaded_orders_path,
                            os.path.join(process_path, 'original'),
                            file_name)

    # Write accuzip dbf files for this job, and save copy to accuzip folder
    write_azzuzip_files(eddm_order, process_path, file_name, match_search[1])
    # update processing files table, set processing date
    get_order_by_date.extended_update_processing_file_table(gblv, file_name, eddm_order)
    get_order_by_date.status_update_processing_file_table(gblv, file_name, "Hard match, order processed")
    # Copy file to complete_processing_files path
    move_file_to_new_folder(gblv.downloaded_orders_path,
                            gblv.complete_processing_path, file_name, delete_original=gblv.delete_original_files)

    get_order_by_date.append_filename_to_orderdetail(gblv)
    get_order_by_date.processing_table_to_history(gblv)
    write_message_log()


def unlock_file_routes(file_name):
    """
    Manually opens routes for file_name.  File must still exist in gblv.complete_processing_path
    Updates file processing history status to DELETED [Date]
    """
    global gblv
    gblv = settings.GlobalVar()
    # Set environment to 'PRODUCTION' for production
    # gblv.set_environment('QA')
    gblv.set_environment('PRODUCTION')
    gblv.set_order_paths()
    gblv.create_accuzip_dir()
    gblv.set_token_name()
    gblv.set_db_name()

    if not os.path.exists(os.path.join(gblv.complete_processing_path, file_name)):
        print("'{0}' not found in {1}".format(file_name, gblv.complete_processing_path))
        time.sleep(4)
        return -1

    qry_resl = get_order_by_date.qry_processing_files_history(gblv, file_name)
    if int(qry_resl[0][0]) != 0:
        file_name = qry_resl[0][1]
        job = qry_resl[0][2]
        qty = qry_resl[0][3]

        get_order_by_date.delete_orders_table(gblv)
        gblv.print_log("\tUnlocking routes for {}".format(file_name))
        # All all records from old orders into delete_order_records table
        with open(os.path.join(gblv.complete_processing_path, file_name), 'r') as o:
            csvr = csv.DictReader(o, ['AgentID', 'DateSelected', 'City', 'State',
                                      'ZipCode', 'RouteID', 'Quantity', 'POS',
                                      'NumberOfTouches', 'SessionID'], delimiter='\t')
            next(csvr)
            for line in csvr:
                get_order_by_date.insert_into_delete_orders_table(gblv, file_name, line)

        # create set of session ids to unlock
        session_id = get_order_by_date.get_session_id_sqlite(gblv, 'delete_order_records')
        # iterate through sessions ids and unlock routes
        get_order_by_date.delete_order_record_unlock_routes(gblv, session_id)
        # update file history status
        process_date = datetime.datetime.strftime(datetime.datetime.today(), "%m/%d/%Y")
        get_order_by_date.status_update_processing_history_table(gblv, file_name, "JOB CANCELLED, Routes unlocked"
                                                                 " {}".format(process_date))

    else:
        print("No order match found for file {}".format(file_name))
        time.sleep(4)


def search_v2fbluserdata(search_field, search_string):
    """
    """
    global gblv
    gblv = settings.GlobalVar()
    # Set environment to 'PRODUCTION' for production
    # gblv.set_environment('QA')
    gblv.set_environment('PRODUCTION')
    gblv.set_order_paths()
    gblv.create_accuzip_dir()
    gblv.set_token_name()
    gblv.set_db_name()
    results = get_order_by_date.search_v2fbl(gblv, search_field, search_string)

    if results:
        with open("V2FBLUserData Search Results.txt", "w+") as s:
            s.write("Query Results:\n\n")
            for n, result in enumerate(results, 1):
                for k, v in result.items():
                    s.write("\t{0}: {1}\n".format(k, v))
                s.write("\n")
    else:
        print("No match in V2FBLUserData for {0} = {1}".format(search_field, search_string))
        time.sleep(4)


def search_jobs(criteria, search_string):
    """
    """
    global gblv
    gblv = settings.GlobalVar()
    # Set environment to 'PRODUCTION' for production
    # gblv.set_environment('QA')
    gblv.set_environment('PRODUCTION')
    gblv.set_order_paths()
    gblv.create_accuzip_dir()
    gblv.set_token_name()
    gblv.set_db_name()

    where_clause = ["", "WHERE a.user_id = '{}'".format(search_string), 
                    "WHERE c.lname LIKE '%{}%'".format(search_string), 
                    "WHERE a.order_order_number = '{}'".format(search_string),
                    "WHERE d.filename = '{}'".format(search_string)]

    results = get_order_by_date.search_jobs(gblv, where_clause[criteria])

    if results:
        with open("Job Search Results.txt", "w+") as s:
            s.write("Query Results:\n\n")
            for n, result in enumerate(results, 1):
                for k, v in result.items():
                    s.write("\t{0}: {1}\n".format(k, v))
                s.write("\n")
    else:
        print("No live job match for search criteria:\n{0}".format(where_clause[criteria]))
        time.sleep(4)


def cancel_order(order_order_number):
    """
    Updates OrderDetail table for matching order number, sets file_match to 'CANCELLED'
    """
    global gblv
    gblv = settings.GlobalVar()
    # Set environment to 'PRODUCTION' for production
    # gblv.set_environment('QA')
    gblv.set_environment('PRODUCTION')
    gblv.set_order_paths()
    gblv.create_accuzip_dir()
    gblv.set_token_name()
    gblv.set_db_name()
    status = get_order_by_date.cancel_order_detail_order(gblv, order_order_number, "JOB CANCELLED")

    if status:
        print("Cancelled order {}".format(order_order_number))
        time.sleep(4)
    else:
        print("No order match found for order {}".format(order_order_number))
        time.sleep(4)


if __name__ == '__main__':
    run_processing()
