# Python2/3 connector for SQream DB

WARNING: This is a pre-alpha connector. It has not been thoroughly tested,
but should work with SQream DB v1.12

Usage example:

Import the `pysqream.py` file into your own codebase::

        import pysqream as sq
        import atexit
        from datetime import date, datetime
        
        
        sc = sq.connector()
        atexit.register(sc.close)
        sc.connect(host='<ip>', database='<database>', user='<username>', password='<password>',
                    port=<server port>, clustered=<True/False - based on your installation>, timeout=<socket timeout>)
        qr = sc.query("SELECT x,y FROM t")
        print(sc.cols_names())
        print(sc.cols_types())
        print(sc.cols_to_rows())

