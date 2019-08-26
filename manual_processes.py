import time
import fb_run_eddm


def force_process_task():
    print_string = ("\nForce Processing\n"
                    "\tForce processing of file with order in `OrderDetail`.`order_detail_id`\n"
                    "\tforce_processing function called with file name and order detail id\n"
                    "\tex: fb_run_eddm.force_processing('40193_201907082212.dat', '36043422')\n"
                    "\tFile must exist in \\JTSRV2\\Grimes\\FB_EDDM_Orders\\unprocessed_orders)\n")

    print(print_string)

    file_name = input("file name: ")

    if file_name == '':
        print("Enter file name")
        time.sleep(2)
        main()

    order_detail_id = input("order detail id: ")

    if order_detail_id == '':
        print("Enter order detail id")
        time.sleep(2)
        main()

    fb_run_eddm.force_processing(file_name.strip(), order_detail_id.strip())
    print("\n\n")
    main()


def unlock_routes_task():
    print_string = ("\nUnlock file routes\n"
                    "\tTo manually unlock routes in a *.dat file, the file must first\n" 
                    "\texist in \\JTSRV2\\Grimes\\FB_EDDM_Orders\\complete_processing_files\n"
                    "\tThis will unlock the routes in the MSSQLServer database and\n" 
                    "\tupdate `ProcessingFilesHistory`.`status` to 'Job Cancelled, Routes Unlocked'.\n"
                    "\tCall fb_run_eddm.unlock_file_routes() with file name that contains "
                    "routes to unlock\n")

    print(print_string)

    file_name = input("file name: ")

    if file_name == '':
        print("Enter file name")
        time.sleep(2)
        main()

    fb_run_eddm.unlock_file_routes(file_name.strip())
    print("\n\n")
    main()


def job_search_task():
    print_string = ("\nSearch for job information\n\n"
                    "Search for jobs by criteria:\n"
                    "\t1: Agent ID\n"
                    "\t2: Agent Last Name\n"
                    "\t3: Order Number (ex: FB161788)\n"
                    "\t4: File name (ex: '10063_20190718191231.dat')\n")

    print(print_string)

    try:
        search_criteria = int(input("\nSearch criteria (ex: 1): "))

        if search_criteria not in range(1, 5):
            print("\nEnter search number from 1-4\n")
            time.sleep(2)
            main()

    except ValueError:
        print("\nEnter search field by number from 1-4\n")
        time.sleep(4)
        main()

    search_string = input("Search string: ")

    if search_string == '':
        print("\nEnter search string")
        time.sleep(2)
        main()

    fb_run_eddm.search_jobs(search_criteria, search_string.strip())
    print("\n\n")
    main()


def search_v2_task():
    print_string = ("\nSearch V2FBLUserData\n\n"
                    "Search V2FBLUserData for near match in field:\n"
                    "\t1: Agent ID\n"
                    "\t2: First Name\n"
                    "\t3: Last Name\n"
                    "\t4: email\n")

    print(print_string)

    try:
        search_field = int(input("\nSearch by field (ex: 1): "))

        if search_field not in range(1, 6):
            print("\nEnter search field by number from 1-5\n")
            time.sleep(2)
            main()

    except ValueError:
        print("\nEnter search field by number from 1-5\n")
        time.sleep(4)
        main()

    search_string = input("Search string: ")

    if search_string == '':
        print("\nEnter search string")
        time.sleep(2)
        main()

    fields = ['', 'agent_id', 'fname', 'lname', 'agent_email']

    fb_run_eddm.search_v2fbluserdata(fields[search_field], search_string.strip())
    print("\n\n")
    main()


def cancel_order_task():
    print_string = ("\nCancel Order\n"
                    "\tCancelling an order will update `OrderDetail`.`file_match` to\n"
                    "\t'JOB CANCELLED'.  When a job is cancelled, the script will no longer\n"
                    "\tattempt to find a match to files in the non-match orders.  This should\n"
                    "\tbe done to keep the reports clear of error messages and maintain\n" 
                    "\ta clean database.  Call function fb_run_eddm.cancel_order() with\n" 
                    "\tthe order detail id from `OrderDetail`.`order_order_number`.\n"
                    "\tEx: fb_run_eddm.cancel_order('FB161309')\n")

    print(print_string)

    order_number = input("order detail id: ")

    if order_number == '':
        print("Enter order detail id")
        time.sleep(2)
        main()

    fb_run_eddm.cancel_order(order_number.strip().upper())
    print("\n\n")


def main():
    print_string = ("Data table references by [Table Name].[Field Name]\n"
                    "Table can be viewed / edited with SQLite Expert Personal application\n"
                    "\tTable path: \\JTSRV2\\Grimes\\FB_EDDM_Orders\\eddm_db.db\n"
                    "\n\t1: Force Processing\n\t"
                    "2: Unlock Routes\n\t3: Cancel Order\n\t"
                    "4: Search V2FBLUserData file\n\t"
                    "5: Job search"
                    "\n\nChoose task (0 to exit): ")

    try:
        ans = int(input(print_string))

        if ans == 0:
            print("\nbye")
            time.sleep(1.5)
            exit()

        if ans not in range(1, 6):
            print("Enter task by number from 1-5\n\n")
            time.sleep(2)
            main()

    except ValueError:
        print("Enter task by number from 1-5\n\n")
        time.sleep(2)
        main()

    tasks = [exit, force_process_task, unlock_routes_task, 
             cancel_order_task, search_v2_task, job_search_task]

    tasks[ans]()


if __name__ == '__main__':
    main()
