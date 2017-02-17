
import pysqream3 as sq

if __name__ == "__main__":
    sc = sq.connector()

    sc.connect(host='192.168.0.161'
               ,database='master'
               ,user='sqream'
               ,password='sqream'
               ,port=5000
               ,timeout=15,
              clustered=True) # You can set the timeout to be longer if desired

    # It is good practice to surround the queries in error handling Try/Except...
    #try:
        # The result type for the below should be "None", because they are
        # statements, not queries...

        #sc.query("create table test(x int not null)")
        #sc.query("insert into test values (1),(2),(3),(4)")

    #except:
    #    print "Couldn't create a table and insert values..."

    # Here, the result won't be None, because we are expecting results...
    # The sc object will always contain the last result columns, so we don't
    # have to pass it explicitly from the result of the query.
    #
    # If we have a few queries, we can pass the result from sc.query over.
    # For example,
    # q1 = sc.query("select * from test")
    # q2 = sc.query("select top 5 * from other_table")
    # print sc.cols_to_rows(q1)
    # print sc.cols_to_rows(q2)

    q1 = sc.query("select * from test")

    # Column names. I am not passing q1 explicitly...

    print sc.cols_names()
    # Column types - one of
    #  ftUByte - tinyint
    #  ftShort - smallint
    #  ftInt - int
    #  ftLong - bigint
    #  ftFloat - float/real
    #  ftDouble - double/float
    #  ftBool - bit/boolean
    #  ftDate - Date
    #  ftDateTime - DateTime/Timestamp
    #  ftVarchar - VarChar
    print sc.cols_types()
    # Print the result as rows of data:
    print sc.cols_to_rows()
    # Finally, drop the table we created
    #try:
    #    sc.query("drop table test")
    #except:
    #    print "Couldn't drop table"

    # And close the connection
    #sc.close()

# Other functions you may use:
#  sc.last_query() - returns the string of the last query/statement executed
#  sc.last_cols() - returns the column objects from the last query/statement executed
