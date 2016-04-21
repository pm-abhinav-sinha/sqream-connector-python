
import pysqream as sq

if __name__ == "__main__":
    sc = sq.connector()
    sc.connect(host='192.168.0.161',database='hourly_quick',user='sqream',password='sqream',port=5000,timeout=15)
    try:
        sc.query("drop table floof")
    except:
        pass
    #sc.query("create table floof(x int not null, y int not null)")
    sc.query("create table floof as (select top 50 xint,xdate,xdatetime,xvarchar40 from t_a)")
    sc.query("select * from floof")
    print sc.cols_names()
    print sc.cols_types()
    print sc.cols_to_rows()

