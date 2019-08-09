import fb_run_eddm


def main():
    pass

if __name__ == '__main__':
    main()
    """
    Text with leading # are commented
        # comment
    Commented text is ignored by the python interpreter
    Text within triple quotes are also commented out
    Data table references by [Table Name].[Field Name]
    Table can be viewed / edited with SQLite Expert Personal application
    table path: \\JTSRV2\\Grimes\\FB_EDDM_Orders\\eddm_db.db
    """
    # 
    """
    Force Processing
        Force processing of file with order in `OrderDetail`.`order_detail_id`
        update force_processing function with file name and order detail id
        ex: fb_run_eddm.force_processing('40193_201907082212.dat', '36043422')
        File must exist in \\JTSRV2\\Grimes\\FB_EDDM_Orders\\unprocessed_orders
    """
    # 
    # fb_run_eddm.force_processing('[file name]', '[order id]')
    # 
    """
    Unlock file routes
        To manually unlock routes in a *.dat file, the file must first 
        exist in \\JTSRV2\\Grimes\\FB_EDDM_Orders\\complete_processing_files
        This will unlock the routes in the MSSQLServer database and 
        update `ProcessingFilesHistory`.`status` to 'Job Cancelled, Routes Unlocked'.
        Call fb_run_eddm.unlock_file_routes() with file name that will be used 
        to unlock routes.
    """
    # 
    # fb_run_eddm.unlock_file_routes('[file name]')
    # 
    """
    Cancel Order
        Cancelling an order will update `OrderDetail`.`file_match` to
        'JOB CANCELLED'.  When a job is cancelled, the script will no longer
        attempt to find a match to files in the non-match orders.  This should
        be done to keep the reports clear of error messages and maintain 
        a clean database.  Call function fb_run_eddm.cancel_order() with 
        the order detail id from `OrderDetail`.`order_detail_id`.
    """
    # 
    # fb_run_eddm.cancel_order('order detail id')
    # 
